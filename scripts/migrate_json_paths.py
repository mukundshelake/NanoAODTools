#!/usr/bin/env python3
"""
migrate_json_paths.py
=====================
Convert all dataFiles JSONs in Datasets/ from **absolute paths** to paths
that are **relative to the appropriate storage root**.

  • Paths that start with INPUT_STORAGE  → stored relative to INPUT_STORAGE
  • Paths that start with OUTPUT_STORAGE → stored relative to OUTPUT_STORAGE
  • Paths already relative (or that match neither root) → left unchanged

After running this script, main_new.py (with .env properly configured)
will resolve the relative paths back to absolute at runtime, making the
entire pipeline portable between TIFR and lxplus with a single .env swap.

Usage
-----
    python3 scripts/migrate_json_paths.py [--dry-run] [--datasets-dir DATASETS_DIR]

Options
-------
    --dry-run        Print what would change without writing any files.
    --datasets-dir   Path to Datasets folder (default: Datasets/ next to repo root).
    --input-storage  Override INPUT_STORAGE  (default: read from .env / os.environ).
    --output-storage Override OUTPUT_STORAGE (default: read from .env / os.environ).

A backup of each JSON is written to Datasets/backup_absolute/ before any
modification.  Run the script again (without --dry-run) to apply; it is
idempotent — already-relative paths are skipped.
"""

import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# .env loader  (duplicated here so the script works standalone)
# ---------------------------------------------------------------------------
def _load_env(env_file=None):
    repo_root = Path(__file__).resolve().parent.parent
    if env_file is None:
        env_file = repo_root / ".env"
    else:
        env_file = Path(env_file)
    loaded = {}
    if not env_file.exists():
        return loaded
    with open(env_file) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            loaded[key] = value
            if key not in os.environ:
                os.environ[key] = value
    return loaded


# ---------------------------------------------------------------------------

def make_relative(path: str, root: str) -> str:
    """Return path relative to root, or path unchanged if it doesn't start with root."""
    root = root.rstrip("/")
    if path.startswith(root + "/") or path == root:
        return os.path.relpath(path, root)
    return path


def migrate_json(json_path: Path, input_storage: str, output_storage: str,
                 backup_dir: Path, dry_run: bool) -> int:
    """Migrate one JSON file.  Returns number of paths converted."""
    with open(json_path) as f:
        data = json.load(f)

    converted = 0
    new_data = {}
    for dm, keys in data.items():
        new_data[dm] = {}
        for key, files_dict in keys.items():
            new_data[dm][key] = {}
            for path, tree_name in files_dict.items():
                if not os.path.isabs(path):
                    # Already relative — keep as-is
                    new_data[dm][key][path] = tree_name
                    continue
                new_path = path
                if input_storage and path.startswith(input_storage.rstrip("/") + "/"):
                    new_path = make_relative(path, input_storage)
                    converted += 1
                elif output_storage and path.startswith(output_storage.rstrip("/") + "/"):
                    new_path = make_relative(path, output_storage)
                    converted += 1
                else:
                    logging.warning(f"  Path matches neither storage root — kept absolute: {path[:80]}")
                new_data[dm][key][new_path] = tree_name

    if converted == 0:
        logging.info(f"  {json_path.name}: already portable (0 conversions needed)")
        return 0

    if dry_run:
        logging.info(f"  [DRY RUN] {json_path.name}: would convert {converted} path(s)")
        return converted

    # Backup original
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / json_path.name
    if not backup_path.exists():
        shutil.copy2(json_path, backup_path)
        logging.info(f"  Backed up to {backup_path.relative_to(json_path.parent.parent)}")

    with open(json_path, "w") as f:
        json.dump(new_data, f, indent=4)
    logging.info(f"  {json_path.name}: converted {converted} path(s)  ✓")
    return converted


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    _load_env()

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument("--dry-run", action="store_true",
                        help="Print changes without writing files.")
    parser.add_argument("--datasets-dir", default=str(repo_root / "Datasets"),
                        help="Path to Datasets folder.")
    parser.add_argument("--input-storage", default=os.environ.get("INPUT_STORAGE", ""),
                        help="Absolute path used as the base for nfs_skimmed paths.")
    parser.add_argument("--output-storage", default=os.environ.get("OUTPUT_STORAGE", ""),
                        help="Absolute path used as the base for processed-stage paths.")
    args = parser.parse_args()

    input_storage  = args.input_storage.rstrip("/")
    output_storage = args.output_storage.rstrip("/")

    if not input_storage or not output_storage:
        logging.error(
            "ERROR: INPUT_STORAGE and OUTPUT_STORAGE must both be set.\n"
            "  Either set them in .env or pass --input-storage / --output-storage."
        )
        sys.exit(1)

    datasets_dir = Path(args.datasets_dir)
    if not datasets_dir.exists():
        logging.error(f"Datasets directory not found: {datasets_dir}")
        sys.exit(1)

    backup_dir = datasets_dir / "backup_absolute"
    json_files = sorted(datasets_dir.glob("*_dataFiles.json"))

    if not json_files:
        logging.info("No *_dataFiles.json files found.")
        sys.exit(0)

    mode = "[DRY RUN] " if args.dry_run else ""
    logging.info(f"\n{mode}Migrating {len(json_files)} dataFiles JSON(s)")
    logging.info(f"  INPUT_STORAGE  = {input_storage}")
    logging.info(f"  OUTPUT_STORAGE = {output_storage}")
    if not args.dry_run:
        logging.info(f"  Backups        → {backup_dir}\n")
    else:
        logging.info("")

    total_converted = 0
    for jf in json_files:
        total_converted += migrate_json(jf, input_storage, output_storage, backup_dir, args.dry_run)

    logging.info(f"\n{'[DRY RUN] ' if args.dry_run else ''}Total paths converted: {total_converted}")
    if args.dry_run and total_converted:
        logging.info("Re-run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
