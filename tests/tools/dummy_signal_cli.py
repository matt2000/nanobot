#!/usr/bin/env python3
"""
Dummy signal-cli replacement for testing.

Mimics the CLI interface used by ``SignalChannel`` in both modes:

    signal-cli -a ACCOUNT send -m "msg" RECIPIENT
    signal-cli -a ACCOUNT send -m "msg" -g GROUP_ID
    signal-cli -a ACCOUNT receive --json -t 1
    signal-cli -a ACCOUNT daemon --json

Behaviour:
- **send** stores the message body and queues an echo envelope so
  that the next ``receive`` invocation returns it.
- **receive --json** outputs any queued envelopes as newline-delimited
  JSON, then exits.
- **daemon --json** stays running, reads JSON-RPC requests from stdin,
  and writes echo envelopes + RPC responses to stdout.

State (poll mode) is persisted via a tiny JSON spool file so that
independent send/receive invocations share the queue — exactly like the
real signal-cli whose state lives on disk.
"""

import json
import os
import sys
import time
import fcntl
from pathlib import Path
from tempfile import gettempdir

SPOOL = Path(os.environ.get("DUMMY_SIGNAL_SPOOL", Path(gettempdir()) / "dummy_signal_cli_spool.json"))


def _read_spool() -> list[dict]:
    if not SPOOL.exists():
        return []
    with open(SPOOL) as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _write_spool(items: list[dict]) -> None:
    with open(SPOOL, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(items, f)
        fcntl.flock(f, fcntl.LOCK_UN)


def _make_envelope(sender: str, chat_id: str, text: str, is_group: bool) -> dict:
    """Build a signal-cli-style JSON envelope."""
    data_msg: dict = {
        "message": text,
        "timestamp": int(time.time() * 1000),
    }
    if is_group:
        data_msg["groupInfo"] = {"groupId": chat_id}
    return {
        "envelope": {
            "source": sender,
            "sourceNumber": sender,
            "sourceName": "DummyCLI",
            "dataMessage": data_msg,
        }
    }


def cmd_send(account: str, args: list[str]) -> None:
    """Handle: signal-cli -a ACCOUNT send -m MSG [-g GROUP] [RECIPIENT]"""
    message = ""
    recipient = ""
    group_id = ""
    i = 0
    while i < len(args):
        if args[i] == "-m" and i + 1 < len(args):
            message = args[i + 1]
            i += 2
        elif args[i] == "-g" and i + 1 < len(args):
            group_id = args[i + 1]
            i += 2
        else:
            recipient = args[i]
            i += 1

    if not message:
        print("error: missing -m", file=sys.stderr)
        sys.exit(1)

    is_group = bool(group_id)
    chat_id = group_id if is_group else recipient
    echo_text = f"Echo: {message}"

    # Determine who the "echo" comes from.
    # In a real scenario the bot sent the message, and the echo simulates
    # the *other side* replying, so source is the recipient/group.
    echo_sender = chat_id if is_group else recipient

    envelope = _make_envelope(echo_sender, chat_id, echo_text, is_group)
    spool = _read_spool()
    spool.append(envelope)
    _write_spool(spool)


def cmd_receive() -> None:
    """Handle: signal-cli -a ACCOUNT receive --json [-t TIMEOUT]"""
    spool = _read_spool()
    _write_spool([])  # drain
    for env in spool:
        print(json.dumps(env), flush=True)


def cmd_daemon() -> None:
    """Handle: signal-cli -a ACCOUNT daemon --json

    Stays running, reads JSON-RPC requests from stdin, and for each
    ``send`` request writes an echo envelope + success response to stdout.
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue

        method = req.get("method", "")
        params = req.get("params", {})
        rpc_id = req.get("id")

        if method == "send":
            message = params.get("message", "")
            recipients = params.get("recipient", [])
            group_id = params.get("groupId", "")

            echo_text = f"Echo: {message}"
            is_group = bool(group_id)
            chat_id = group_id if is_group else (recipients[0] if recipients else "")
            source = chat_id

            envelope = _make_envelope(source, chat_id, echo_text, is_group)
            # Override sourceName for daemon mode
            envelope["envelope"]["sourceName"] = "DummyRPC"

            # Write echo envelope (simulating an incoming message)
            print(json.dumps(envelope), flush=True)

            # Write JSON-RPC success response
            resp = {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {"timestamp": 1700000000000},
            }
            print(json.dumps(resp), flush=True)
        else:
            resp = {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }
            print(json.dumps(resp), flush=True)


def main() -> None:
    args = sys.argv[1:]

    # Parse global -a ACCOUNT flag
    account = ""
    i = 0
    while i < len(args):
        if args[i] == "-a" and i + 1 < len(args):
            account = args[i + 1]
            rest = args[:i] + args[i + 2:]
            break
        i += 1
    else:
        rest = args

    if not rest:
        print("usage: dummy_signal_cli -a ACCOUNT <send|receive|clean>", file=sys.stderr)
        sys.exit(1)

    command = rest[0]
    cmd_args = rest[1:]

    if command == "clean":
        SPOOL.unlink(missing_ok=True)
    elif command == "send":
        cmd_send(account, cmd_args)
    elif command == "receive":
        cmd_receive()
    elif command == "daemon":
        cmd_daemon()
    else:
        print(f"unknown command: {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()