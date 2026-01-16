#!/usr/bin/env python3
"""
Download and manage golden JSON files for data certification.

Usage:
    python scripts/download_golden_jsons.py [--check] [--force]

Options:
    --check: Check which files exist without downloading
    --force: Re-download even if files exist
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import utils


def main():
    parser = argparse.ArgumentParser(description='Manage golden JSON files')
    parser.add_argument('--check', action='store_true',
                       help='Check status without downloading')
    parser.add_argument('--force', action='store_true',
                       help='Re-download even if files exist')
    args = parser.parse_args()
    
    config_path = Path(__file__).parent.parent / 'config.yaml'
    config = utils.load_config(config_path)
    
    golden_jsons = config.get('golden_jsons', {})
    output_dir = Path('data/golden_jsons')
    
    if not golden_jsons:
        print("No golden JSONs configured in config.yaml")
        return 1
    
    if args.check:
        print("Golden JSON file status:")
        status = utils.validate_golden_jsons(config)
        for year, info in sorted(status.items()):
            exists_str = "✓ EXISTS" if info['exists'] else "✗ MISSING"
            print(f"  {year}: {info['filename']} - {exists_str}")
        
        all_exist = all(info['exists'] for info in status.values())
        return 0 if all_exist else 1
    
    # Download (skip existing unless --force)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading golden JSON files to {output_dir}/\n")
    
    for year, json_info in golden_jsons.items():
        filename = json_info['filename']
        url = json_info['url']
        # Save with .json extension
        json_filename = Path(filename).stem + '.json'
        filepath = output_dir / json_filename
        
        # Skip if exists and not --force
        if filepath.exists() and not args.force:
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"✓ {json_filename} (already exists, {size_mb:.2f} MB)")
            continue
        
        try:
            print(f"↓ Downloading {filename} for {year}...", end=' ', flush=True)
            import urllib.request
            urllib.request.urlretrieve(url, filepath)
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"✓ ({size_mb:.2f} MB) -> {json_filename}")
        except Exception as e:
            print(f"✗ Error: {e}")
            return 1
    
    print("\n✓ All golden JSON files ready!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
