"""
API key registration utility.

Interactive utility to register and encrypt API keys using KeyManager.

Usage
-----
python -m temp_utili.register_keys --passphrase <passphrase>

This will start an interactive session where you can:
- Add new keys
- List existing keys
- Delete keys
- Verify key values
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from key_manager import KeyManager


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register and manage encrypted API keys."
    )
    parser.add_argument(
        "--passphrase",
        required=True,
        help="Passphrase for key encryption/decryption.",
    )
    parser.add_argument(
        "--storage-path",
        default="secrets/keys.json",
        help="Path to key storage file (default: secrets/keys.json).",
    )

    args = parser.parse_args()

    # Ensure secrets directory exists
    storage_path = Path(args.storage_path)
    storage_path.parent.mkdir(parents=True, exist_ok=True)

    km = KeyManager(
        storage_path=args.storage_path,
        passphrase=args.passphrase,
        auto_persist=True,
    )

    print("=" * 60)
    print("API Key Registration Utility")
    print("=" * 60)
    print(f"Storage: {args.storage_path}")
    print()
    print("Commands:")
    print("  add     - Add a new key")
    print("  list    - List all keys")
    print("  get     - Retrieve a key value")
    print("  delete  - Delete a key")
    print("  quit    - Exit")
    print("=" * 60)
    print()

    while True:
        try:
            command = input("Command> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting...")
            break

        if command == "quit" or command == "q":
            print("Goodbye!")
            break

        elif command == "add":
            key_name = input("  Key name: ").strip()
            if not key_name:
                print("  Error: Key name cannot be empty")
                continue
            key_value = input("  Key value: ").strip()
            if not key_value:
                print("  Error: Key value cannot be empty")
                continue
            try:
                km.set(key_name, key_value)
                print(f"  Key '{key_name}' registered successfully")
            except Exception as exc:
                print(f"  Error: {exc}")

        elif command == "list":
            keys = km.list_keys()
            if keys:
                print("  Registered keys:")
                for key in keys:
                    print(f"    - {key}")
            else:
                print("  No keys registered")

        elif command == "get":
            key_name = input("  Key name: ").strip()
            if not key_name:
                print("  Error: Key name cannot be empty")
                continue
            try:
                value = km.get(key_name)
                if value is None:
                    print(f"  Key '{key_name}' not found")
                else:
                    # Show masked value for security
                    masked = value[:4] + "..." + value[-4:] if len(value) > 8 else "****"
                    print(f"  Value: {masked}")
                    show_full = input("  Show full value? (y/n): ").strip().lower()
                    if show_full == "y":
                        print(f"  Full value: {value}")
            except Exception as exc:
                print(f"  Error: {exc}")

        elif command == "delete":
            key_name = input("  Key name: ").strip()
            if not key_name:
                print("  Error: Key name cannot be empty")
                continue
            confirm = input(f"  Delete '{key_name}'? (y/n): ").strip().lower()
            if confirm == "y":
                if km.delete(key_name):
                    print(f"  Key '{key_name}' deleted")
                else:
                    print(f"  Key '{key_name}' not found")
            else:
                print("  Cancelled")

        elif command == "":
            continue

        else:
            print(f"  Unknown command: {command}")
            print("  Available: add, list, get, delete, quit")


if __name__ == "__main__":
    main()
