"""Verification entry point: full regression plus independent reconciliation.

The original suite is run unmodified. The reconciliation script advances its
expected rows only from successful commit receipts, and the public fixtures and
demonstration are exercised too. Any failure exits non-zero.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(argv, label):
    print("== " + label + " ==", flush=True)
    completed = subprocess.run(argv, cwd=ROOT)
    if completed.returncode != 0:
        raise SystemExit("%s failed with exit code %d"
                         % (label, completed.returncode))


def main():
    python = sys.executable
    run([python, "-B", "-m", "unittest", "discover",
         "-s", "tests", "-t", ".", "-v"], "full regression")
    run([python, "-B", "reconcile_rebucket.py"], "independent history audit")
    run([python, "-B", "rebucket_examples.py", "--check-fixtures"],
        "public fixture checks")
    run([python, "-B", "rebucket_examples.py", "--demo"],
        "public rebucket example")
    run([python, "-B", "demo.py"], "legacy demo")
    print("VERIFY_OK")


if __name__ == "__main__":
    main()
