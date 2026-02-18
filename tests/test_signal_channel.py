"""Tests for Signal channel – both daemon and poll modes.

Uses ``tests/tools/dummy_signal_cli.py`` to avoid needing a real
signal-cli installation.  The dummy script supports ``send``, ``receive``,
and ``daemon`` subcommands.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
import textwrap
from pathlib import Path

import pytest

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.signal import SignalChannel
from nanobot.config.schema import SignalConfig

TOOLS_DIR = Path(__file__).resolve().parent / "tools"
DUMMY_CLI = TOOLS_DIR / "dummy_signal_cli.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(
    *,
    mode: str = "poll",
    signal_cli: str = "",
    account: str = "+15550001111",
    allow_from: list[str] | None = None,
    poll_interval: int = 1,
) -> SignalConfig:
    return SignalConfig(
        enabled=True,
        account=account,
        signal_cli=signal_cli or f"{sys.executable} {DUMMY_CLI}",
        mode=mode,
        poll_interval=poll_interval,
        allow_from=allow_from or [],
    )


def _make_wrapper(target_script: Path, spool_path: Path | None = None) -> Path:
    """Create a tiny shell wrapper so that ``shutil.which`` finds a real
    executable on $PATH and the channel can invoke it directly."""
    wrapper = target_script.parent / f"_wrapper_{target_script.stem}.sh"
    env_line = ""
    if spool_path:
        env_line = f'export DUMMY_SIGNAL_SPOOL="{spool_path}"\n'
    wrapper.write_text(
        f"#!/usr/bin/env bash\n"
        f"{env_line}"
        f'exec "{sys.executable}" "{target_script}" "$@"\n'
    )
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)
    return wrapper


# ---------------------------------------------------------------------------
# Unit: envelope processing
# ---------------------------------------------------------------------------

class TestProcessEnvelope:
    """Test ``_process_envelope`` in isolation (no subprocess needed)."""

    @pytest.mark.asyncio
    async def test_processes_dm_envelope(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="+10000000000", signal_cli="false")
        ch = SignalChannel(cfg, bus)

        envelope = {
            "envelope": {
                "source": "+15551234567",
                "sourceNumber": "+15551234567",
                "sourceName": "Alice",
                "dataMessage": {
                    "message": "Hello bot",
                    "timestamp": 1700000000000,
                },
            }
        }

        await ch._process_envelope(envelope)

        msg: InboundMessage = bus.inbound.get_nowait()
        assert msg.channel == "signal"
        assert msg.content == "Hello bot"
        assert msg.chat_id == "+15551234567"
        assert msg.sender_id == "+15551234567|Alice"
        assert msg.metadata["is_group"] is False

    @pytest.mark.asyncio
    async def test_processes_group_envelope(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="+10000000000", signal_cli="false")
        ch = SignalChannel(cfg, bus)

        envelope = {
            "envelope": {
                "source": "+15559999999",
                "sourceNumber": "+15559999999",
                "sourceName": "Bob",
                "dataMessage": {
                    "message": "Group hello",
                    "timestamp": 1700000000000,
                    "groupInfo": {"groupId": "abc123=="},
                },
            }
        }

        await ch._process_envelope(envelope)

        msg: InboundMessage = bus.inbound.get_nowait()
        assert msg.chat_id == "abc123=="
        assert msg.metadata["is_group"] is True
        assert msg.metadata["group_id"] == "abc123=="

    @pytest.mark.asyncio
    async def test_ignores_receipt_envelope(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="+10000000000", signal_cli="false")
        ch = SignalChannel(cfg, bus)

        # Receipt – no dataMessage
        envelope = {
            "envelope": {
                "source": "+15551234567",
                "sourceNumber": "+15551234567",
                "receiptMessage": {"type": "DELIVERY"},
            }
        }

        await ch._process_envelope(envelope)
        assert bus.inbound.empty()

    @pytest.mark.asyncio
    async def test_ignores_empty_message(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="+10000000000", signal_cli="false")
        ch = SignalChannel(cfg, bus)

        # dataMessage exists but message is empty (e.g. reaction)
        envelope = {
            "envelope": {
                "source": "+15551234567",
                "sourceNumber": "+15551234567",
                "dataMessage": {"timestamp": 1700000000000},
            }
        }

        await ch._process_envelope(envelope)
        assert bus.inbound.empty()

    @pytest.mark.asyncio
    async def test_sender_without_name(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="+10000000000", signal_cli="false")
        ch = SignalChannel(cfg, bus)

        envelope = {
            "envelope": {
                "source": "+15550000000",
                "sourceNumber": "+15550000000",
                "dataMessage": {
                    "message": "anon msg",
                    "timestamp": 1700000000000,
                },
            }
        }

        await ch._process_envelope(envelope)
        msg = bus.inbound.get_nowait()
        # No pipe separator when name is absent
        assert msg.sender_id == "+15550000000"


# ---------------------------------------------------------------------------
# Unit: is_allowed
# ---------------------------------------------------------------------------

class TestIsAllowed:

    def test_allow_all_when_empty(self) -> None:
        ch = SignalChannel(
            SignalConfig(enabled=True, account="+1", signal_cli="false"),
            MessageBus(),
        )
        assert ch.is_allowed("+15550001111") is True

    def test_allow_list_grants_access(self) -> None:
        ch = SignalChannel(
            SignalConfig(enabled=True, account="+1", signal_cli="false",
                         allow_from=["+15551111111"]),
            MessageBus(),
        )
        assert ch.is_allowed("+15551111111") is True
        assert ch.is_allowed("+19999999999") is False
        # sender_id may contain "|Name"
        assert ch.is_allowed("+15551111111|Alice") is True
        assert ch.is_allowed("+19999999999|Bob") is False


# ---------------------------------------------------------------------------
# Unit: _is_group_id helper
# ---------------------------------------------------------------------------

class TestIsGroupId:

    def test_phone_is_not_group(self) -> None:
        assert SignalChannel._is_group_id("+15551234567") is False

    def test_base64_is_group(self) -> None:
        assert SignalChannel._is_group_id("abc123==") is True

    def test_empty_is_not_group(self) -> None:
        assert SignalChannel._is_group_id("") is False


# ---------------------------------------------------------------------------
# Integration: poll mode with dummy_signal_cli
# ---------------------------------------------------------------------------

class TestPollMode:
    """End-to-end tests using ``dummy_signal_cli.py``."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path: Path) -> None:
        self.spool = tmp_path / "spool.json"
        self.wrapper = _make_wrapper(DUMMY_CLI, self.spool)

    @pytest.fixture(autouse=True)
    def _teardown(self) -> None:
        yield
        if self.wrapper.exists():
            self.wrapper.unlink()

    @pytest.mark.asyncio
    async def test_send_and_receive_echo(self) -> None:
        bus = MessageBus()
        cfg = _make_config(
            mode="poll",
            signal_cli=str(self.wrapper),
            poll_interval=1,
        )
        ch = SignalChannel(cfg, bus)

        # Send a message via CLI mode
        outbound = OutboundMessage(
            channel="signal",
            chat_id="+15559998888",
            content="ping",
        )
        await ch.send(outbound)

        # Now poll to pick up the echo
        await ch._poll_once()

        msg: InboundMessage = bus.inbound.get_nowait()
        assert msg.content == "Echo: ping"
        assert msg.chat_id == "+15559998888"

    @pytest.mark.asyncio
    async def test_send_to_group(self) -> None:
        bus = MessageBus()
        cfg = _make_config(
            mode="poll",
            signal_cli=str(self.wrapper),
        )
        ch = SignalChannel(cfg, bus)

        outbound = OutboundMessage(
            channel="signal",
            chat_id="mygroup==",
            content="group msg",
        )
        await ch.send(outbound)
        await ch._poll_once()

        msg = bus.inbound.get_nowait()
        assert msg.content == "Echo: group msg"
        assert msg.metadata["is_group"] is True

    @pytest.mark.asyncio
    async def test_poll_with_no_messages(self) -> None:
        bus = MessageBus()
        cfg = _make_config(
            mode="poll",
            signal_cli=str(self.wrapper),
        )
        ch = SignalChannel(cfg, bus)

        # Poll when spool is empty — should not crash
        await ch._poll_once()
        assert bus.inbound.empty()

    @pytest.mark.asyncio
    async def test_access_denied_not_published(self) -> None:
        """Messages from senders not in allow_from are silently dropped."""
        bus = MessageBus()
        cfg = _make_config(
            mode="poll",
            signal_cli=str(self.wrapper),
            allow_from=["+10000000000"],  # only allow this number
        )
        ch = SignalChannel(cfg, bus)

        # Send from +15559998888 (not in allow_from)
        outbound = OutboundMessage(channel="signal", chat_id="+15559998888", content="hi")
        await ch.send(outbound)
        await ch._poll_once()

        # The echo comes from +15559998888 which is NOT allowed
        assert bus.inbound.empty()


# ---------------------------------------------------------------------------
# Integration: daemon mode with dummy_signal_rpc
# ---------------------------------------------------------------------------

class TestDaemonMode:
    """End-to-end tests using the ``daemon`` subcommand of ``dummy_signal_cli.py``."""

    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.wrapper = _make_wrapper(DUMMY_CLI)

    @pytest.mark.asyncio
    async def test_daemon_send_receives_echo(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(
            enabled=True,
            account="+15550001111",
            signal_cli=str(self.wrapper),
            mode="daemon",
        )
        ch = SignalChannel(cfg, bus)

        # Manually launch the daemon subprocess (without blocking on start())
        cmd = [str(self.wrapper), "-a", cfg.account, "daemon", "--json"]
        ch._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        ch._running = True

        # Start reading stdout in background
        reader = asyncio.create_task(ch._read_daemon_stdout())

        # Send via RPC
        await ch._rpc_send("+15559998888", "daemon ping")

        # Give the dummy a moment to write the echo
        await asyncio.sleep(0.3)

        # Should have received the echo envelope
        assert not bus.inbound.empty()
        msg: InboundMessage = bus.inbound.get_nowait()
        assert msg.content == "Echo: daemon ping"

        # Clean up
        ch._running = False
        if ch._proc.stdin:
            ch._proc.stdin.close()
        reader.cancel()
        try:
            await reader
        except asyncio.CancelledError:
            pass
        try:
            ch._proc.terminate()
            await asyncio.wait_for(ch._proc.wait(), timeout=2)
        except Exception:
            ch._proc.kill()

    @pytest.mark.asyncio
    async def test_daemon_send_to_group(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(
            enabled=True,
            account="+15550001111",
            signal_cli=str(self.wrapper),
            mode="daemon",
        )
        ch = SignalChannel(cfg, bus)

        cmd = [str(self.wrapper), "-a", cfg.account, "daemon", "--json"]
        ch._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        ch._running = True
        reader = asyncio.create_task(ch._read_daemon_stdout())

        await ch._rpc_send("groupABC==", "daemon group msg")
        await asyncio.sleep(0.3)

        msg = bus.inbound.get_nowait()
        assert msg.content == "Echo: daemon group msg"
        assert msg.metadata["is_group"] is True

        ch._running = False
        if ch._proc.stdin:
            ch._proc.stdin.close()
        reader.cancel()
        try:
            await reader
        except asyncio.CancelledError:
            pass
        try:
            ch._proc.terminate()
            await asyncio.wait_for(ch._proc.wait(), timeout=2)
        except Exception:
            ch._proc.kill()


# ---------------------------------------------------------------------------
# Unit: start guards
# ---------------------------------------------------------------------------

class TestStartGuards:

    @pytest.mark.asyncio
    async def test_start_rejects_missing_account(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="", signal_cli="false")
        ch = SignalChannel(cfg, bus)
        await ch.start()
        assert ch.is_running is False

    @pytest.mark.asyncio
    async def test_start_rejects_missing_binary(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(
            enabled=True,
            account="+15550001111",
            signal_cli="/nonexistent/signal-cli-xxx",
        )
        ch = SignalChannel(cfg, bus)
        await ch.start()
        assert ch.is_running is False

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self) -> None:
        bus = MessageBus()
        cfg = SignalConfig(enabled=True, account="+1", signal_cli="false")
        ch = SignalChannel(cfg, bus)
        # Calling stop without start should not raise
        await ch.stop()
        assert ch.is_running is False
