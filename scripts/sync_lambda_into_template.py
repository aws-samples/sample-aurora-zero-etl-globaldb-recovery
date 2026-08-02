#!/usr/bin/env python3
"""Keep the inline Lambda code in the CloudFormation template identical to the
standalone source file.

The template embeds lambda/zero_etl_recovery.py in the RecoveryFunction
Code.ZipFile block so the stack deploys with a single command and no S3 bucket.
The standalone file remains the readable, testable, lintable copy. This script
is the guard that stops the two from drifting apart.

Usage
-----
    python3 scripts/sync_lambda_into_template.py --check   # exit 1 if drifted
    python3 scripts/sync_lambda_into_template.py --write   # regenerate block
"""

import argparse
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCE = REPO_ROOT / "lambda" / "zero_etl_recovery.py"
TEMPLATE = REPO_ROOT / "cloudformation" / "zero-etl-recovery.yaml"

BANNER = "# GENERATED FROM lambda/zero_etl_recovery.py - DO NOT EDIT"
ZIPFILE_RE = re.compile(r"^(?P<indent>[ ]*)ZipFile: \|[ ]*$")


def _split_template(lines):
    """Return (before, indent, after) around the ZipFile block."""
    for index, line in enumerate(lines):
        match = ZIPFILE_RE.match(line)
        if not match:
            continue
        indent = len(match.group("indent"))
        end = index + 1
        while end < len(lines):
            candidate = lines[end]
            if candidate.strip() == "":
                end += 1
                continue
            leading = len(candidate) - len(candidate.lstrip(" "))
            if leading <= indent:
                break
            end += 1
        return lines[: index + 1], indent, lines[end:]
    raise SystemExit(f"No 'ZipFile: |' block found in {TEMPLATE}")


def _render_block(source_text, indent):
    pad = " " * (indent + 2)
    rendered = [f"{pad}{BANNER}"]
    for line in source_text.splitlines():
        rendered.append(f"{pad}{line}" if line.strip() else "")
    return rendered


def _current_block(lines, indent):
    """Extract the embedded source from an existing block, minus the banner."""
    pad = indent + 2
    body = []
    for line in lines:
        if line.strip() == BANNER:
            continue
        body.append(line[pad:] if len(line) > pad else "")
    # Drop leading and trailing blank padding for a stable comparison.
    while body and body[0] == "":
        body.pop(0)
    while body and body[-1] == "":
        body.pop()
    return "\n".join(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="verify only")
    group.add_argument("--write", action="store_true", help="regenerate block")
    args = parser.parse_args()

    source_text = SOURCE.read_text(encoding="utf-8").rstrip("\n")
    template_lines = TEMPLATE.read_text(encoding="utf-8").splitlines()

    before, indent, after = _split_template(template_lines)
    existing_block = template_lines[len(before) : len(template_lines) - len(after)]

    if args.check:
        embedded = _current_block(existing_block, indent)
        if embedded == source_text:
            print("OK: template inline code matches lambda/zero_etl_recovery.py")
            return 0
        print(
            "DRIFT: the template's inline code differs from "
            "lambda/zero_etl_recovery.py.\n"
            "Run: python3 scripts/sync_lambda_into_template.py --write",
            file=sys.stderr,
        )
        return 1

    rebuilt = before + _render_block(source_text, indent) + after
    TEMPLATE.write_text("\n".join(rebuilt) + "\n", encoding="utf-8")
    print(
        f"Wrote {len(source_text.splitlines())} lines of Lambda source into "
        f"{TEMPLATE.relative_to(REPO_ROOT)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
