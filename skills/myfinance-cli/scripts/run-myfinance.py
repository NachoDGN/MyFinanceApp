#!/usr/bin/env python3
"""Normalize MyFinanceApp CLI invocations and forward them to the repo CLI."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PLACEHOLDER = "__codex__"


def find_workspace(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / "apps/cli/src/index.ts").exists() and (candidate / "package.json").exists():
            return candidate
    return None


def resolve_workspace(raw_workspace: str | None) -> Path:
    if raw_workspace:
        workspace = Path(raw_workspace).expanduser().resolve()
        if not (workspace / "apps/cli/src/index.ts").exists():
            raise SystemExit(f"Workspace does not look like MyFinanceApp: {workspace}")
        return workspace

    workspace = find_workspace(Path.cwd().resolve())
    if workspace is None:
        raise SystemExit(
            "Could not find a MyFinanceApp workspace from the current directory. "
            "Pass --workspace /path/to/MyFinanceApp."
        )
    return workspace


def rewrite_literal_command(argv: list[str], literal: str) -> list[str]:
    if len(argv) == 1:
        return [argv[0], literal]
    if argv[1].startswith("-"):
        return [argv[0], literal, *argv[1:]]
    return argv


def rewrite_metrics(argv: list[str]) -> list[str]:
    if len(argv) >= 2 and argv[1] == "get" and (len(argv) == 2 or argv[2].startswith("-")):
        return ["metrics", "--help"]
    if len(argv) >= 3 and argv[1] == "get":
        return ["metrics", argv[2], PLACEHOLDER, *argv[3:]]
    if len(argv) >= 2 and not argv[1].startswith("-"):
        if len(argv) == 2 or argv[2].startswith("-"):
            return ["metrics", argv[1], PLACEHOLDER, *argv[2:]]
    return argv


def rewrite_transaction(argv: list[str]) -> list[str]:
    if len(argv) >= 2 and argv[1] == "update" and (len(argv) == 2 or argv[2].startswith("-")):
        return ["transaction", "--help"]
    if len(argv) >= 3 and argv[1] == "update":
        return ["transaction", argv[2], PLACEHOLDER, *argv[3:]]
    if len(argv) >= 2 and not argv[1].startswith("-"):
        if len(argv) == 2 or argv[2].startswith("-"):
            return ["transaction", argv[1], PLACEHOLDER, *argv[2:]]
    return argv


def rewrite_command(argv: list[str]) -> list[str]:
    if not argv:
        return argv

    head = argv[0]
    if head == "dashboard":
        return rewrite_literal_command(argv, "summary")
    if head == "metrics":
        return rewrite_metrics(argv)
    if head == "insights":
        return rewrite_literal_command(argv, "list")
    if head == "transactions":
        return rewrite_literal_command(argv, "list")
    if head == "transaction":
        return rewrite_transaction(argv)
    return argv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the MyFinanceApp CLI using its intended command syntax.",
    )
    parser.add_argument(
        "--workspace",
        help="Path to the MyFinanceApp repository root. Defaults to the nearest matching ancestor.",
    )
    parser.add_argument(
        "--show-command",
        action="store_true",
        help="Print the raw forwarded command to stderr before executing it.",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    command = list(args.command)

    if command and command[0] == "--":
        command = command[1:]

    if not command:
        parser.error("Provide the MyFinance command to run.")

    workspace = resolve_workspace(args.workspace)
    forwarded = rewrite_command(command)
    raw_command = [
        "pnpm",
        "--dir",
        str(workspace),
        "--filter",
        "@myfinance/cli",
        "exec",
        "tsx",
        "src/index.ts",
        *forwarded,
    ]

    if args.show_command:
        print("Forwarding:", " ".join(raw_command), file=sys.stderr)

    completed = subprocess.run(raw_command, cwd=workspace)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
