"""Generate the infrastructure credentials for Debezium, Grafana and the notification broker.

Phase 3 adds three credentials that are not service-client secrets (those come
from `generate_service_secrets.py`). This script writes one gitignored file each:

    infra/secrets/infra/debezium.properties        password=<value>
    infra/secrets/infra/grafana_ro.secret          <value>
    infra/secrets/infra/notification_broker.secret <value>

Which Compose service reads which file (mounted as Compose secrets only, never as
environment variables or baked into an image):

    debezium.properties         db-init (sets the `debezium` role password) and
                                connect (Kafka Connect FileConfigProvider reads the
                                `password=` line)                      - plan 03-05
    grafana_ro.secret           db-init (sets the read-only Grafana role password)
                                - plan 03-05; grafana (PostgreSQL datasource)
                                - plan 03-16
    notification_broker.secret  the notification processes and rabbitmq-init, for
                                the least-privilege RabbitMQ user       - plan 03-14

Safety properties (STRIDE T-3-03):
    - Each file is created mode 0600 and the directory 0700.
    - Files are created with O_EXCL: an existing file is never overwritten unless
      `--force` is given (rotating a credential locks its consumers out until both
      sides reload it). If any file exists without `--force`, nothing is written.
    - It prints the output PATHS only, never a generated value.
    - `infra/secrets/` is gitignored; these files must never be committed, baked
      into a Docker image or logged.

Usage (from the repo root):

    python scripts/generate_infra_secrets.py
    python scripts/generate_infra_secrets.py --output-dir /path/to/dir --force

Exit codes: 0 on success, 1 when a file exists and `--force` is absent.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys

DEFAULT_OUTPUT_DIR = "infra/secrets/infra"

# file name -> format string for the file content (one `{value}` placeholder).
SECRET_FILES: dict[str, str] = {
    "debezium.properties": "password={value}\n",
    "grafana_ro.secret": "{value}",
    "notification_broker.secret": "{value}",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate the Debezium, Grafana read-only and notification broker credentials."
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"directory for the secret files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite existing secrets (rotates them)"
    )
    args = parser.parse_args(argv)

    output_dir = args.output_dir
    paths = {name: os.path.join(output_dir, name) for name in SECRET_FILES}
    existing = [path for path in paths.values() if os.path.exists(path)]
    if existing and not args.force:
        for path in existing:
            print(f"ERROR: {path} already exists; refusing to overwrite it.", file=sys.stderr)
        print("Re-run with --force to rotate every infrastructure secret.", file=sys.stderr)
        return 1

    os.makedirs(output_dir, mode=0o700, exist_ok=True)
    # 0o700 is owner-only (the stricter choice); the rule flags any value above 0o644.
    os.chmod(output_dir, 0o700)  # nosemgrep: python.lang.security.audit.insecure-file-permissions.insecure-file-permissions

    for name, template in SECRET_FILES.items():
        path = paths[name]
        # O_EXCL without --force keeps "refuse to overwrite" race-free; fchmod
        # fixes the mode of a pre-existing file under --force.
        flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if args.force else os.O_EXCL)
        fd = os.open(path, flags, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(template.format(value=secrets.token_urlsafe(32)))
        print(f"Wrote infrastructure secret to {path} (mode 0600).")

    print("These files are gitignored and must never be committed or shared.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
