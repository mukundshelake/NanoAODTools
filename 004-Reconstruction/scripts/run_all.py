#!/usr/bin/env python3
"""
Master orchestration script for 004-Reconstruction Data/MC plotting workflow

Follows the 002-Samples pattern with hash-based output directories for reproducibility.

Usage:
    # Basic usage (reads from config.yaml)
    python scripts/run_all.py
    
    # Override config settings
    python scripts/run_all.py --era UL2017 --tag midNov
    
    # Plot specific variables only
    python scripts/run_all.py --variables Top_lep_pt,Top_had_mass,Chi2
    
    # Force regeneration with same config
    python scripts/run_all.py --force
    
    # Tag this run for easy reference
    python scripts/run_all.py --tag-run baseline
"""

import argparse
import sys
import subprocess
from pathlib import Path

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import utils


def run_command(cmd, description):
    """Run a shell command and handle errors"""
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")
    print(f"Command: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    
    if result.returncode != 0:
        print(f"\n✗ Error: {description} failed with return code {result.returncode}")
        sys.exit(1)
    
    print(f"\n✓ {description} completed successfully")
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Master script to generate reconstruction Data/MC plots",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Config overrides
    parser.add_argument('--eras', type=str,
                        help='Override eras from config.yaml (comma-separated, e.g., "UL2017,UL2018")')
    parser.add_argument('--tag', type=str,
                        help='Override tag from config.yaml')
    parser.add_argument('--no-filter', action='store_true',
                        help='Disable chi2_status==0 filter')
    parser.add_argument('--variables', type=str,
                        help='Comma-separated list of variables to plot (overrides config)')
    
    # Workflow control
    parser.add_argument('--force', action='store_true',
                        help='Force regeneration even if config hash exists')
    parser.add_argument('--tag-run', type=str,
                        help='Create named tag symlink for this run (e.g., "baseline", "paper_v1")')
    parser.add_argument('--skip-histograms', action='store_true',
                        help='Skip histogram generation (only create plots from existing .coffea)')
    parser.add_argument('--skip-plots', action='store_true',
                        help='Skip plotting (only create histograms)')
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = Path(__file__).parent.parent / 'config.yaml'
    print(f"Loading configuration from: {config_path}")
    config = utils.load_config(config_path)
    
    # Apply command-line overrides
    if args.eras:
        config['analysis']['eras'] = args.eras.split(',')
    if args.tag:
        config['analysis']['tag'] = args.tag
    if args.no_filter:
        config['analysis']['apply_chi2_filter'] = False
    if args.variables:
        config['variables'] = args.variables.split(',')
    
    # Extract configuration
    eras_config = config['analysis'].get('eras', config['analysis'].get('era'))
    # Ensure eras is always a list
    if isinstance(eras_config, str):
        eras = [eras_config]
    else:
        eras = eras_config
    
    tag = config['analysis']['tag']
    apply_chi2_filter = config['analysis'].get('apply_chi2_filter', True)
    variables = utils.get_variables_to_plot(config)
    
    print(f"\nConfiguration:")
    print(f"  Era(s): {', '.join(eras)}")
    print(f"  Tag: {tag}")
    print(f"  Chi2 filter: {apply_chi2_filter}")
    print(f"  Variables: {len(variables)} ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
    
    # Process each era
    for era in eras:
        print(f"\n{'='*80}")
        print(f"Processing Era: {era}")
        print(f"{'='*80}")
        
        process_era(era, tag, apply_chi2_filter, variables, config_path, args)
    
    print(f"\n{'='*80}")
    print(f"ALL ERAS COMPLETED")
    print(f"{'='*80}")
    print(f"Processed {len(eras)} era(s): {', '.join(eras)}")
    print(f"✓ Workflow completed successfully!")


def process_era(era, tag, apply_chi2_filter, variables, config_path, args):
    
    # Compute config hash
    config_hash = utils.compute_config_hash(config_path)
    print(f"\nConfig hash: {config_hash}")
    
    # Setup output directory
    base_dir = Path(__file__).parent.parent / 'outputs'
    output_dir = base_dir / config_hash
    
    # Determine expected coffea filename
    expected_coffea = utils.format_coffea_filename(tag, era)
    expected_coffea_path = output_dir / expected_coffea
    
    if output_dir.exists() and not args.force:
        print(f"\n⚠ Output directory already exists: {output_dir}")
        print("  Use --force to regenerate, or modify config.yaml for a new run")
        
        # Check if the specific expected .coffea file exists
        if expected_coffea_path.exists() and not args.skip_histograms:
            print(f"  Found existing .coffea file: {expected_coffea}")
            response = input("  Skip histogram generation and proceed to plotting? [Y/n]: ")
            if response.lower() not in ['n', 'no']:
                args.skip_histograms = True
    else:
        utils.setup_output_dir(base_dir, config_hash, config_path)
        print(f"✓ Created output directory: {output_dir}")
    
    # Update latest symlink
    utils.update_latest_symlink(base_dir, config_hash)
    print(f"✓ Updated 'latest' symlink → {config_hash}")
    
    # Create tagged symlink if requested
    if args.tag_run:
        tags_dir = base_dir / 'tags'
        tags_dir.mkdir(exist_ok=True)
        tag_link = tags_dir / args.tag_run
        if tag_link.exists() or tag_link.is_symlink():
            tag_link.unlink()
        tag_link.symlink_to(f'../{config_hash}', target_is_directory=True)
        print(f"✓ Created tag: {args.tag_run} → {config_hash}")
    
    # Step 1: Generate histograms with RecoDataMCHist.py
    if not args.skip_histograms:
        cmd = [
            sys.executable,  # Use same Python interpreter
            'RecoDataMCHist.py',
            '-e', era,
            '-t', tag,
        ]
        if not apply_chi2_filter:
            cmd.append('--no-filter')
        
        run_command(cmd, "Step 1: Generate histograms (RecoDataMCHist.py)")
        
        # Move .coffea file to hash-based output directory
        source_file = Path(__file__).parent.parent / 'outputs' / expected_coffea
        if source_file.exists():
            import shutil
            dest_file = output_dir / source_file.name
            shutil.move(str(source_file), str(dest_file))
            print(f"✓ Moved {source_file.name} to {config_hash}/")
    else:
        print("\n⊘ Skipping histogram generation (--skip-histograms)")
    
    # Step 2: Generate plots with RecoHistPlotter.py
    if not args.skip_plots:
        # Check if the specific expected .coffea file exists
        if not expected_coffea_path.exists():
            print(f"\n✗ Error: Expected .coffea file not found: {expected_coffea_path}")
            sys.exit(1)
        
        print(f"\nProcessing: {expected_coffea}")
        
        coffea_file = expected_coffea_path
            
        # Create plots subdirectory in hash output dir
        plots_dir = output_dir / 'plots'
        plots_dir.mkdir(exist_ok=True)
        
        cmd = [
            sys.executable,
            'RecoHistPlotter.py',
            str(coffea_file),
            '--output-dir', str(plots_dir),
        ]
        
        run_command(cmd, f"Step 2: Generate plots for {expected_coffea}")
        
        print(f"\n✓ All plots saved to: {plots_dir}")
    else:
        print("\n⊘ Skipping plot generation (--skip-plots)")
    
    # Log this run
    run_history = Path(__file__).parent.parent / 'run_history.txt'
    utils.log_run(run_history, config_hash, {
        'era': era,
        'tag': tag,
        'variables': variables,
        'apply_chi2_filter': apply_chi2_filter
    })
    print(f"\n✓ Logged run to run_history.txt")
    
    # Summary for this era
    print(f"\n{'='*80}")
    print(f"SUMMARY for {era}")
    print(f"{'='*80}")
    print(f"Config hash: {config_hash}")
    print(f"Outputs: outputs/{config_hash}/")
    print(f"Plots: outputs/{config_hash}/plots/")
    print(f"Latest: outputs/latest/ → {config_hash}")
    if args.tag_run:
        print(f"Tagged as: {args.tag_run}")
    print()
    
    # Count generated files
    if output_dir.exists():
        coffea_count = len(list(output_dir.glob('*.coffea')))
        plots_count = len(list((output_dir / 'plots').glob('*.png'))) if (output_dir / 'plots').exists() else 0
        print(f"Generated files:")
        print(f"  {coffea_count} .coffea file(s)")
        print(f"  {plots_count} .png plot(s)")


if __name__ == '__main__':
    main()
