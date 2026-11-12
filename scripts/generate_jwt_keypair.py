"""Generate Identity's RSA signing key (RS256) for local development.

Identity is the ONLY service that holds a private key; every other service
verifies tokens against the public JWKS Identity publishes. This script writes
the private key to a gitignored file that Identity loads from
`JWT_PRIVATE_KEY_PATH` (default `infra/secrets/jwt_private.pem`).

Safety properties (STRIDE T-01.1-03):
    - The file is created mode 0600 and its parent directory 0700.
    - It refuses to overwrite an existing key unless `--force` is given —
      replacing the key invalidates every token Identity has already issued.
    - It prints the output PATH and a reminder only, never key material.
    - `infra/secrets/` and `*.pem` are gitignored; the key must never be
      committed, baked into a Docker image, or logged.

Usage (from the repo root):

    python scripts/generate_jwt_keypair.py
    python scripts/generate_jwt_keypair.py --output /path/to/key.pem --force

Exit codes: 0 on success, 1 when the output file exists and `--force` is absent.
"""

from __future__ import annotations

import argparse
import os
import sys

# Repo root on sys.path so this works whether invoked as
# `python scripts/generate_jwt_keypair.py` or via an absolute path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.identity.security.keys import generate_private_key_pem  # noqa: E402

DEFAULT_OUTPUT = "infra/secrets/jwt_private.pem"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Identity's RS256 private key (PKCS8 PEM).")
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT, help=f"where to write the key (default: {DEFAULT_OUTPUT})"
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite an existing key (invalidates issued tokens)"
    )
    args = parser.parse_args(argv)

    output = args.output
    parent = os.path.dirname(os.path.abspath(output))
    os.makedirs(parent, mode=0o700, exist_ok=True)

    # O_EXCL without --force makes "refuse to overwrite" race-free; with
    # --force the file is truncated. fchmod fixes the mode of a pre-existing
    # file, since the os.open() mode argument only applies on creation.
    flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if args.force else os.O_EXCL)
    try:
        fd = os.open(output, flags, 0o600)
    except FileExistsError:
        print(
            f"ERROR: {output} already exists; refusing to overwrite it "
            "(replacing the key invalidates every token already issued). "
            "Re-run with --force if that is what you want.",
            file=sys.stderr,
        )
        return 1

    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(generate_private_key_pem())

    print(f"Wrote RSA private key to {output} (mode 0600).")
    print("This file is gitignored and must never be committed or shared.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
