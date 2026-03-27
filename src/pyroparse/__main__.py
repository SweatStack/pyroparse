"""CLI entry point: ``pyroparse convert``, ``pyroparse dump``, and ``python -m pyroparse``."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyroparse._convert import DEFAULT_GLOB, convert_fit_file, convert_fit_tree


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyroparse",
        description="Fast and opinionated activity data parsing.",
    )
    sub = parser.add_subparsers(dest="command")

    # -- convert ---------------------------------------------------------------
    convert = sub.add_parser(
        "convert",
        help="convert FIT files to Parquet",
        description="Convert FIT files to Parquet — single files or entire directory trees.",
    )
    convert.add_argument(
        "src",
        type=Path,
        help="FIT file or directory of FIT files",
    )
    convert.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="output file or directory (default: .parquet next to source)",
    )
    convert.add_argument(
        "--overwrite",
        action="store_true",
        help="re-convert files whose output already exists",
    )
    convert.add_argument(
        "--glob",
        default=DEFAULT_GLOB,
        metavar="PATTERN",
        help=f"file discovery pattern (default: {DEFAULT_GLOB})",
    )
    convert.add_argument(
        "-w", "--workers",
        type=int,
        default=1,
        metavar="N",
        help="parallel workers; -1 = all cores (default: 1)",
    )
    convert.add_argument(
        "--no-progress",
        action="store_true",
        help="disable the progress bar",
    )

    # -- dump ------------------------------------------------------------------
    dump = sub.add_parser(
        "dump",
        help="dump raw FIT messages as JSON",
        description="Dump every message in a FIT file as JSON. No pyroparse opinions applied.",
    )
    dump.add_argument(
        "src",
        type=Path,
        help="FIT file to dump",
    )
    dump.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        metavar="FILE",
        help="write to file instead of stdout",
    )
    kind_group = dump.add_mutually_exclusive_group()
    kind_group.add_argument(
        "--kind",
        default=None,
        metavar="TYPE[,TYPE,...]",
        help="only include these message types (comma-separated)",
    )
    kind_group.add_argument(
        "--exclude",
        default=None,
        metavar="TYPE[,TYPE,...]",
        help="exclude these message types (comma-separated)",
    )
    dump.add_argument(
        "--compact",
        action="store_true",
        help="single-line JSON (default: pretty-printed)",
    )

    return parser


def _cmd_convert(args: argparse.Namespace) -> int:
    src = args.src.expanduser()
    dst = args.output.expanduser() if args.output else None

    if not src.exists():
        print(f"error: {src} does not exist", file=sys.stderr)
        return 1

    # Single file: direct conversion, no tree walk.
    if src.is_file():
        if dst is None:
            dst = src.with_suffix(".parquet")
        try:
            result = convert_fit_file(src, dst)
        except Exception as exc:
            print(f"error: {src}: {exc}", file=sys.stderr)
            return 1
        for p in (result if isinstance(result, list) else [result]):
            print(p)
        return 0

    # Directory: batch conversion.
    result = convert_fit_tree(
        src,
        dst,
        glob=args.glob,
        overwrite=args.overwrite,
        workers=args.workers,
        progress=not args.no_progress,
    )

    for path, exc in result.errors:
        print(f"error: {path}: {exc}", file=sys.stderr)

    n_ok = len(result.converted)
    n_err = len(result.errors)
    n_total = n_ok + n_err

    if n_total == 0:
        print("nothing to convert (all files up to date)")
    elif n_err == 0:
        print(f"converted {n_ok} file{'s' * (n_ok != 1)}")
    else:
        print(
            f"converted {n_ok}/{n_total} file{'s' * (n_total != 1)} "
            f"({n_err} failed)",
            file=sys.stderr,
        )

    return 1 if result.failed else 0


def _cmd_dump(args: argparse.Namespace) -> int:
    from pyroparse._messages import all_messages

    src = args.src.expanduser()

    if not src.is_file():
        print(f"error: {src} does not exist or is not a file", file=sys.stderr)
        return 1

    try:
        msgs = all_messages(src)
    except Exception as exc:
        print(f"error: {src}: {exc}", file=sys.stderr)
        return 1

    # Apply --kind / --exclude filters.
    if args.kind is not None:
        kinds = {k.strip() for k in args.kind.split(",")}
        msgs = [m for m in msgs if m["kind"] in kinds]
    elif args.exclude is not None:
        exclude = {k.strip() for k in args.exclude.split(",")}
        msgs = [m for m in msgs if m["kind"] not in exclude]

    indent = None if args.compact else 2

    if args.output is not None:
        dst = args.output.expanduser()
        dst.parent.mkdir(parents=True, exist_ok=True)
        with open(dst, "w") as f:
            json.dump(msgs, f, indent=indent, default=str)
            f.write("\n")
    else:
        json.dump(msgs, sys.stdout, indent=indent, default=str)
        sys.stdout.write("\n")

    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "convert":
        sys.exit(_cmd_convert(args))
    elif args.command == "dump":
        sys.exit(_cmd_dump(args))


if __name__ == "__main__":
    main()
