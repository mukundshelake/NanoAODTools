#!/usr/bin/env python3
"""
Master script to generate all outputs for 002-Samples chapter.

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]
    
Options:
    --force: Regenerate outputs even if config hash already exists
    --tag: Create a named tag symlink to this run (e.g., "baseline", "paper_v1")
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import utils


def generate_dataSamples(config, output_dir, config_hash):
    """Generate dataSamples.json"""
    print("  Generating dataSamples.json...")
    
    # TODO: Implement actual data fetching logic
    # For now, load from placeholder
    placeholder_path = Path('outputs/placeholder/dataSamples.json')
    if placeholder_path.exists():
        import json
        with open(placeholder_path) as f:
            placeholder = json.load(f)
        data = placeholder.get('data', [])
    else:
        data = []
    
    # Save with metadata
    table_id = config['outputs']['dataSamples']['table_id']
    caption = config['outputs']['dataSamples']['caption']
    
    utils.save_output_json(
        output_dir / 'dataSamples.json',
        data,
        table_id,
        caption,
        config_hash,
        'scripts/run_all.py::generate_dataSamples'
    )


def generate_goldenJSONs(config, output_dir, config_hash):
    """Generate goldenJSONs.json and download golden JSON files"""
    print("  Generating goldenJSONs.json...")
    
    # Download golden JSON files
    utils.download_golden_jsons(config)
    
    # Build data for output JSON
    data = []
    for year, golden_json_info in config['golden_jsons'].items():
        # Use .json extension for local file
        original_filename = golden_json_info['filename']
        json_filename = Path(original_filename).stem + '.json'
        
        entry = {
            'year': year,
            'filename': json_filename,
            'source_url': golden_json_info['url'],
            'local_path': f"data/golden_jsons/{json_filename}"
        }
        data.append(entry)
    
    table_id = config['outputs']['goldenJSONs']['table_id']
    caption = config['outputs']['goldenJSONs']['caption']
    
    utils.save_output_json(
        output_dir / 'goldenJSONs.json',
        data,
        table_id,
        caption,
        config_hash,
        'scripts/run_all.py::generate_goldenJSONs'
    )


def generate_MCSamples(config, output_dir, config_hash):
    """Generate MCSamples.json"""
    print("  Generating MCSamples.json...")
    
    # TODO: Implement actual MC sample info fetching
    # For now, load from placeholder
    placeholder_path = Path('outputs/placeholder/MCSamples.json')
    if placeholder_path.exists():
        import json
        with open(placeholder_path) as f:
            placeholder = json.load(f)
        data = placeholder.get('data', [])
    else:
        data = []
    
    table_id = config['outputs']['MCSamples']['table_id']
    caption = config['outputs']['MCSamples']['caption']
    
    utils.save_output_json(
        output_dir / 'MCSamples.json',
        data,
        table_id,
        caption,
        config_hash,
        'scripts/run_all.py::generate_MCSamples'
    )


def generate_SystSamples(config, output_dir, config_hash):
    """Generate SystSamples.json"""
    print("  Generating SystSamples.json...")
    
    # TODO: Implement actual systematic sample info fetching
    placeholder_path = Path('outputs/placeholder/SystSamples.json')
    if placeholder_path.exists():
        import json
        with open(placeholder_path) as f:
            placeholder = json.load(f)
        data = placeholder.get('data', [])
    else:
        data = []
    
    table_id = config['outputs']['SystSamples']['table_id']
    caption = config['outputs']['SystSamples']['caption']
    
    utils.save_output_json(
        output_dir / 'SystSamples.json',
        data,
        table_id,
        caption,
        config_hash,
        'scripts/run_all.py::generate_SystSamples'
    )


def main():
    parser = argparse.ArgumentParser(description='Generate all outputs for 002-Samples')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate even if output directory exists')
    parser.add_argument('--tag', type=str,
                       help='Create named tag for this run (e.g., baseline, paper_v1)')
    args = parser.parse_args()
    
    # Paths
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / 'config.yaml'
    outputs_base = base_dir / 'outputs'
    history_file = base_dir / 'run_history.txt'
    
    # Load config and compute hash
    config = utils.load_config(config_path)
    config_hash = utils.compute_config_hash(config_path)
    
    print(f"Config hash: {config_hash}")
    
    # Create output directory
    output_dir, config_hash, is_new_run = utils.create_output_directory(
        outputs_base, config_path
    )
    
    if not is_new_run and not args.force and not args.tag:
        print(f"Output directory {config_hash}/ already exists.")
        print("Use --force to regenerate, or modify config to create new run.")
        print(f"Output location: {output_dir}")
        return 0
    
    if not is_new_run and args.tag and not args.force:
        print(f"Output directory {config_hash}/ already exists.")
        print("Skipping regeneration, but will create tag.")
    
    if not is_new_run:
        print(f"Regenerating outputs in {config_hash}/ (--force specified)")
    elif is_new_run:
        print(f"Creating new output directory: {config_hash}/")
    
    # Generate outputs (skip if only tagging)
    if is_new_run or args.force:
        print("\nGenerating outputs...")
        
        if config['outputs']['dataSamples']['enabled']:
            generate_dataSamples(config, output_dir, config_hash)
        
        if config['outputs']['goldenJSONs']['enabled']:
            generate_goldenJSONs(config, output_dir, config_hash)
        
        if config['outputs']['MCSamples']['enabled']:
            generate_MCSamples(config, output_dir, config_hash)
        
        if config['outputs']['SystSamples']['enabled']:
            generate_SystSamples(config, output_dir, config_hash)
        
        # Update run history
        utils.update_run_history(
            history_file,
            config_hash,
            {'is_new_run': is_new_run, 'forced': args.force}
        )
        
        # Update latest symlink
        utils.update_latest_symlink(outputs_base, config_hash)
    
    # Create tag if requested
    if args.tag:
        tag_dir = outputs_base / 'tags'
        tag_dir.mkdir(exist_ok=True)
        tag_link = tag_dir / args.tag
        
        if tag_link.exists() or tag_link.is_symlink():
            tag_link.unlink()
        
        tag_link.symlink_to(Path('..') / config_hash)
        print(f"\nCreated tag: {args.tag} -> {config_hash}")
    
    print(f"\n✓ Outputs generated successfully!")
    print(f"  Location: {output_dir}")
    print(f"  Config hash: {config_hash}")
    print(f"  Latest symlink: outputs/latest -> {config_hash}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
