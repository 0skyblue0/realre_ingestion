"""
Central entrypoint for the data ingestion manager.

Usage
-----
# Minimal CLI - only passphrase required
python Ingestion_Manager.py --passphrase <passphrase>

# With options
python Ingestion_Manager.py --passphrase <passphrase> --config config/settings.json --once
python Ingestion_Manager.py --passphrase <passphrase> --dry-run

CLI flags mirror :func:`manager.core.build_arg_parser`.
"""

from manager.core import run_from_cli


def main() -> None:
    run_from_cli()


if __name__ == "__main__":
    main()
