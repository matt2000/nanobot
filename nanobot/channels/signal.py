"""Signal channel implementation using signal-cli JSON-RPC or polling."""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import SignalConfig


class SignalChannel(BaseChannel):
    """
    Signal channel using signal-cli.

    Supports two modes:
    - **daemon** (default): Launches ``signal-cli -a ACCOUNT daemon --json``
      as a subprocess, reading newline-delimited JSON envelopes from stdout
      and writing JSON-RPC send commands to stdin.
    - **poll**: Periodically runs ``signal-cli -a ACCOUNT receive --json``
      and parses the output.  Simpler but higher latency.

    Prerequisites:
        1. Install signal-cli: https://github.com/AsamK/signal-cli
        2. Register / link an account:
           ``signal-cli -a +NUMBER register`` or ``signal-cli link``
    """

    name = "signal"

    def __init__(self, config: SignalConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: SignalConfig = config
        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._poll_task: asyncio.Task | None = None
        self._rpc_id: int = 0
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the Signal channel."""
        if not self.config.account:
            logger.error("Signal account (phone number) not configured")
            return

        cli = self.config.signal_cli
        if not shutil.which(cli):
            logger.error(
                f"signal-cli binary not found ('{cli}'). "
                "Install it from https://github.com/AsamK/signal-cli"
            )
            return

        self._running = True

        if self.config.mode == "daemon":
            await self._start_daemon()
        else:
            await self._start_poll()

    async def stop(self) -> None:
        """Stop the Signal channel and clean up resources."""
        self._running = False

        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._proc:
            logger.info("Stopping signal-cli daemon…")
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except (asyncio.TimeoutError, ProcessLookupError):
                self._proc.kill()
            self._proc = None

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Signal."""
        recipient = msg.chat_id  # phone number or group id

        if self.config.mode == "daemon" and self._proc and self._proc.stdin:
            await self._rpc_send(recipient, msg.content)
        else:
            await self._cli_send(recipient, msg.content)

    async def _rpc_send(self, recipient: str, text: str) -> None:
        """Send via JSON-RPC through the daemon's stdin."""
        if not self._proc or not self._proc.stdin:
            logger.warning("signal-cli daemon not running; cannot send")
            return

        self._rpc_id += 1

        # Determine whether recipient is a group or individual
        if self._is_group_id(recipient):
            params = {
                "groupId": recipient,
                "message": text,
                "account": self.config.account,
            }
        else:
            params = {
                "recipient": [recipient],
                "message": text,
                "account": self.config.account,
            }

        payload = {
            "jsonrpc": "2.0",
            "method": "send",
            "id": self._rpc_id,
            "params": params,
        }

        line = json.dumps(payload) + "\n"
        async with self._write_lock:
            try:
                self._proc.stdin.write(line.encode())
                await self._proc.stdin.drain()
                logger.debug(f"Signal RPC send → {recipient}: {text[:60]}…")
            except Exception as e:
                logger.error(f"Failed to write to signal-cli stdin: {e}")

    async def _cli_send(self, recipient: str, text: str) -> None:
        """Send by invoking signal-cli as a one-shot subprocess."""
        cmd = [self.config.signal_cli, "-a", self.config.account]

        if self._is_group_id(recipient):
            cmd += ["send", "-m", text, "-g", recipient]
        else:
            cmd += ["send", "-m", text, recipient]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            if proc.returncode != 0:
                logger.error(f"signal-cli send failed: {stderr.decode().strip()}")
            else:
                logger.debug(f"Signal CLI send → {recipient}: {text[:60]}…")
        except asyncio.TimeoutError:
            logger.error("signal-cli send timed out")
        except Exception as e:
            logger.error(f"signal-cli send error: {e}")

    # ------------------------------------------------------------------
    # Daemon mode
    # ------------------------------------------------------------------

    async def _start_daemon(self) -> None:
        """Launch ``signal-cli daemon --json`` and read envelopes."""
        cmd = [
            self.config.signal_cli,
            "-a", self.config.account,
            "daemon", "--json",
        ]
        logger.info(f"Starting signal-cli daemon: {' '.join(cmd)}")

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            logger.error(f"signal-cli not found at '{self.config.signal_cli}'")
            self._running = False
            return

        # Read stderr in background for diagnostics
        asyncio.create_task(self._log_stderr())

        logger.info("Signal daemon started, listening for messages…")
        self._reader_task = asyncio.create_task(self._read_daemon_stdout())
        await self._reader_task

    async def _read_daemon_stdout(self) -> None:
        """Continuously read JSON envelopes from the daemon's stdout."""
        assert self._proc and self._proc.stdout

        while self._running:
            try:
                raw = await self._proc.stdout.readline()
                if not raw:
                    if self._running:
                        logger.warning("signal-cli daemon stdout closed unexpectedly")
                    break

                line = raw.decode().strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    logger.debug(f"Non-JSON line from signal-cli: {line[:120]}")
                    continue

                await self._process_envelope(data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reading signal-cli output: {e}")
                await asyncio.sleep(1)

    async def _log_stderr(self) -> None:
        """Log signal-cli stderr output for diagnostics."""
        if not self._proc or not self._proc.stderr:
            return
        try:
            while True:
                line = await self._proc.stderr.readline()
                if not line:
                    break
                text = line.decode().strip()
                if text:
                    logger.debug(f"signal-cli stderr: {text}")
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Poll mode
    # ------------------------------------------------------------------

    async def _start_poll(self) -> None:
        """Periodically run ``signal-cli receive --json``."""
        logger.info(
            f"Starting Signal poll mode (every {self.config.poll_interval}s)"
        )
        self._poll_task = asyncio.create_task(self._poll_loop())
        await self._poll_task

    async def _poll_loop(self) -> None:
        """Polling loop that runs signal-cli receive."""
        while self._running:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Signal poll error: {e}")

            await asyncio.sleep(self.config.poll_interval)

    async def _poll_once(self) -> None:
        """Run signal-cli receive once and process envelopes."""
        cmd = [
            self.config.signal_cli,
            "-a", self.config.account,
            "receive", "--json", "-t", "1",
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=30
            )
        except asyncio.TimeoutError:
            logger.warning("signal-cli receive timed out")
            return
        except Exception as e:
            logger.error(f"signal-cli receive error: {e}")
            return

        if proc.returncode != 0:
            err = stderr.decode().strip() if stderr else "unknown error"
            logger.warning(f"signal-cli receive exited {proc.returncode}: {err}")
            return

        if not stdout:
            return

        for line in stdout.decode().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                await self._process_envelope(data)
            except json.JSONDecodeError:
                logger.debug(f"Non-JSON line from signal-cli: {line[:120]}")

    # ------------------------------------------------------------------
    # Envelope processing
    # ------------------------------------------------------------------

    async def _process_envelope(self, data: dict[str, Any]) -> None:
        """
        Parse a signal-cli JSON envelope and forward the message.

        signal-cli outputs envelopes like::

            {
                "envelope": {
                    "source": "+1234567890",
                    "sourceNumber": "+1234567890",
                    "sourceName": "Alice",
                    "dataMessage": {
                        "message": "Hello!",
                        "timestamp": 1700000000000,
                        "groupInfo": { "groupId": "base64==" }
                    }
                }
            }
        """
        envelope = data.get("envelope")
        if not envelope:
            return

        data_msg = envelope.get("dataMessage")
        if not data_msg:
            # Could be a receipt, typing indicator, etc. — skip silently.
            return

        text = data_msg.get("message")
        if not text:
            return

        sender = (
            envelope.get("sourceNumber")
            or envelope.get("source")
            or ""
        )
        sender_name = envelope.get("sourceName", "")

        # Determine chat_id: group id if present, otherwise sender phone
        group_info = data_msg.get("groupInfo") or {}
        group_id = group_info.get("groupId", "")
        chat_id = group_id if group_id else sender

        # Build sender_id with name for display (similar to Telegram pattern)
        sender_id = f"{sender}|{sender_name}" if sender_name else sender

        # Handle attachments
        media_paths: list[str] = []
        for att in data_msg.get("attachments", []):
            file_path = att.get("file")
            if file_path and Path(file_path).exists():
                media_paths.append(file_path)

        logger.debug(f"Signal message from {sender}: {text[:60]}…")

        await self._handle_message(
            sender_id=sender_id,
            chat_id=chat_id,
            content=text,
            media=media_paths,
            metadata={
                "source": sender,
                "source_name": sender_name,
                "timestamp": data_msg.get("timestamp"),
                "group_id": group_id,
                "is_group": bool(group_id),
            },
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_group_id(value: str) -> bool:
        """Check whether a chat_id looks like a Signal group ID (base64)."""
        # Signal group IDs are base64-encoded, typically ending with '='
        # Phone numbers start with '+'
        return bool(value) and not value.startswith("+")
