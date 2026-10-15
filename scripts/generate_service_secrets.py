"""Generate per-service client secrets for Identity's service-token mint (D-06).

Scheduling, Visit and Billing workers have no user context, so they obtain
service tokens from Identity's `POST /internal/service-tokens` by proving who
they are with a per-service secret (HTTP Basic). This script writes one secret
file per client:

    infra/secrets/service-clients/{scheduling,visit,billing}.secret

Identity reads the whole directory (`SERVICE_CLIENT_SECRETS_DIR`); each calling
service reads only its own file (`<SERVICE>_CLIENT_SECRET_PATH`).

Safety properties (STRIDE T-02-04):
    - Each file is created mode 0600 and the directory 0700.
    - It refuses to overwrite an existing secret unless `--force` is given —
      rotating a secret locks that client out until both sides reload it.
    - It prints the output PATHS only, never a secret value.
    - `infra/secrets/` is gitignored; secrets must never be committed, baked
      into a Docker image, or logged.

Usage (from the repo root):

    python scripts/generate_service_secrets.py
    python scripts/generate_service_secrets.py --output-dir /path/to/dir --force

Exit codes: 0 on success, 1 when a secret file exists and `--force` is absent
(no file is written in that case).
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys

DEFAULT_OUTPUT_DIR = "infra/secrets/service-clients"
SERVICE_CLIENTS: tuple[str, ...] = ("scheduling", "visit", "billing")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate per-service client secrets.")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"directory for <service>.secret files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite existing secrets (rotates them)"
    )
    args = parser.parse_args(argv)

    output_dir = args.output_dir
    os.makedirs(output_dir, mode=0o700, exist_ok=True)
    os.chmod(output_dir, 0o700)

    paths = [os.path.join(output_dir, f"{client}.secret") for client in SERVICE_CLIENTS]
    existing = [path for path in paths if os.path.exists(path)]
    if existing and not args.force:
        for path in existing:
            print(f"ERROR: {path} already exists; refusing to overwrite it.", file=sys.stderr)
        print("Re-run with --force to rotate every client secret.", file=sys.stderr)
        return 1

    for path in paths:
        # O_EXCL without --force keeps "refuse to overwrite" race-free; fchmod
        # fixes the mode of a pre-existing file under --force.
        flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if args.force else os.O_EXCL)
        fd = os.open(path, flags, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(secrets.token_urlsafe(32))
        print(f"Wrote client secret to {path} (mode 0600).")

    print("These files are gitignored and must never be committed or shared.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
