#!/usr/bin/env python3
"""
Master orchestration script for 004-Reconstruction Data/MC plotting workflow

Supports both reconstruction and observables analysis types.
Follows the 002-Samples pattern with hash-based output directories for reproducibility.

Usage:
    # Basic usage (reads from config.yaml, runs both analyses)
    python scripts/run_all.py
    
    # Run only reconstruction analysis
    python scripts/run_all.py --analysis-type reco
    
    # Run only observables analysis
    python scripts/run_all.py --analysis-type observables
    
    # Override config settings
    python scripts/run_all.py --eras UL2017 --tag midNov
    
    # Plot specific variables only
    python scripts/run_all.py --variables Top_lep_pt,Top_had_mass,Chi2 --analysis-type reco
    
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
    
    # Run from 004-Reconstruction directory (parent of scripts)
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    
    if result.returncode != 0:
        print(f"\n✗ Error: {description} failed with return code {result.returncode}")
        sys.exit(1)
    
    print(f"\n✓ {description} completed successfully")
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Master script to generate reconstruction and observables Data/MC plots",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Analysis type selection
    parser.add_argument('--analysis-type', type=str, choices=['reco', 'observables', 'bdtvariables', 'bdtvariables_parton', 'all'], default='all',
                        help='Type of analysis to run: reco, observables, bdtvariables, bdtvariables_parton, or all')
    
    # Config overrides
    parser.add_argument('--eras', type=str,
                        help='Override eras from config.yaml (comma-separated, e.g., "UL2017,UL2018")')
    parser.add_argument('--tag', type=str,
                        help='Override tag from config.yaml')
    parser.add_argument('--no-filter', action='store_true',
                        help='Disable chi2_status==0 filter (reco only)')
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
    
    # Determine which analysis types to run
    analysis_types = []
    if args.analysis_type == 'all':
        analysis_types = ['reco', 'observables', 'bdtvariables', 'bdtvariables_parton']
    else:
        analysis_types = [args.analysis_type]
    
    # Apply command-line overrides
    if args.eras:
        config['analysis']['eras'] = args.eras.split(',')
    if args.tag:
        config['analysis']['tag'] = args.tag
    
    # Extract common configuration
    eras_config = config['analysis'].get('eras', config['analysis'].get('era'))
    # Ensure eras is always a list
    if isinstance(eras_config, str):
        eras = [eras_config]
    else:
        eras = eras_config
    
    tag = config['analysis']['tag']
    
    print(f"\nConfiguration:")
    print(f"  Analysis type(s): {', '.join(analysis_types)}")
    print(f"  Era(s): {', '.join(eras)}")
    print(f"  Tag: {tag}")
    
    # Process each analysis type
    for analysis_type in analysis_types:
        print(f"\n{'='*80}")
        print(f"ANALYSIS TYPE: {analysis_type.upper()}")
        print(f"{'='*80}")
        
        # Get analysis-specific config
        analysis_config = config.get(analysis_type, {})
        
        # Apply no-filter override for reco only
        if args.no_filter and analysis_type == 'reco':
            analysis_config['apply_chi2_filter'] = False
        
        apply_chi2_filter = analysis_config.get('apply_chi2_filter', False)
        
        # Get variables for this analysis type
        if args.variables:
            variables = args.variables.split(',')
        else:
            variables = analysis_config.get('variables', [])
        
        print(f"  Chi2 filter: {apply_chi2_filter}")
        print(f"  Variables: {len(variables)} ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
        
        # Process each era for this analysis type
        for era in eras:
            print(f"\n{'-'*80}")
            print(f"Processing: {analysis_type.upper()} - {era}")
            print(f"{'-'*80}")
            
            process_era(era, tag, analysis_type, apply_chi2_filter, variables, config_path, args)
    
    print(f"\n{'='*80}")
    print(f"ALL PROCESSING COMPLETED")
    print(f"{'='*80}")
    print(f"Processed {len(analysis_types)} analysis type(s) × {len(eras)} era(s)")
    print(f"✓ Workflow completed successfully!")


def process_era(era, tag, analysis_type, apply_chi2_filter, variables, config_path, args):
    """
    Process a single era for a given analysis type (reco or observables)
    
    Args:
        era: Era name (e.g., 'UL2017')
        tag: Tag identifier (e.g., 'midNov')
        analysis_type: 'reco' or 'observables'
        apply_chi2_filter: Whether to apply chi2 filter
        variables: List of variables to plot
        config_path: Path to config.yaml
        args: Command line arguments
    """
    
    # Select appropriate scripts based on analysis type
    if analysis_type == 'reco':
        hist_script = 'RecoDataMCHist.py'
        plotter_script = 'RecoHistPlotter.py'
        coffea_suffix = 'reco'
    elif analysis_type == 'observables':
        hist_script = 'ObservablesDataMCHist.py'
        plotter_script = 'ObservablesHistPlotter.py'
        coffea_suffix = 'observables'
    elif analysis_type == 'bdtvariables':
        hist_script = 'BDTvariablesDataMCHist.py'
        plotter_script = 'BDTvariablesHistPlotter.py'
        coffea_suffix = 'bdtvariables'
    elif analysis_type == 'bdtvariables_parton':
        hist_script = 'BDTvariablesPartonProcessor.py'
        plotter_script = 'BDTvariablesPartonPlotter.py'
        coffea_suffix = 'bdtvariables_parton'
    else:
        raise ValueError(f"Unknown analysis type: {analysis_type}")
    
    # Compute config hash (same hash for all analysis types - files have distinguishing names)
    config_hash = utils.compute_config_hash(config_path)
    print(f"\nConfig hash: {config_hash}")
    
    # Setup output directory
    base_dir = Path(__file__).parent.parent / 'outputs'
    output_dir = base_dir / config_hash
    
    # Determine expected coffea filename
    expected_coffea = f"{tag}_{era}_{coffea_suffix}.coffea"
    expected_coffea_path = output_dir / expected_coffea
    
    if output_dir.exists() and not args.force:
        print(f"\n⚠ Output directory already exists: {output_dir}")
        print("  Use --force to regenerate, or modify config.yaml for a new run")
        
        # Check if the specific expected .coffea file exists
        if expected_coffea_path.exists() and not args.skip_histograms:
            print(f"  Found existing .coffea file: {expected_coffea}")
            response = input("  Skip histogram generation and proceed to plotting? [Y/n]: ")
            response = input("  Skip histogram generation and proceed to plotting? [Y/n]: ")
            if response.lower() not in ['n', 'no']:
                args.skip_histograms = True
    else:
        utils.setup_output_dir(base_dir, config_hash, config_path)
        print(f"✓ Created output directory: {output_dir}")
    
    # Update latest symlink (shared across all analysis types)
    utils.update_latest_symlink(base_dir, config_hash)
    print(f"✓ Updated 'latest' symlink → {config_hash}")
    
    # Create tagged symlink if requested (shared, not per-analysis-type)
    if args.tag_run:
        tags_dir = base_dir / 'tags'
        tags_dir.mkdir(exist_ok=True)
        tag_link = tags_dir / args.tag_run
        if tag_link.exists() or tag_link.is_symlink():
            tag_link.unlink()
        tag_link.symlink_to(f'../{config_hash}', target_is_directory=True)
        print(f"✓ Created tag: {args.tag_run} → {config_hash}")
    
    # Step 1: Generate histograms
    if not args.skip_histograms:
        cmd = [
            sys.executable,  # Use same Python interpreter
            f"scripts/{hist_script}",  # Path relative to 004-Reconstruction
            '-e', era,
            '-t', tag,
        ]
        if analysis_type == 'reco' and not apply_chi2_filter:
            cmd.append('--no-filter')
        
        run_command(cmd, f"Step 1: Generate histograms ({hist_script})")
        
        # Move .coffea file to hash-based output directory
        source_file = Path(__file__).parent.parent / 'outputs' / expected_coffea
        if source_file.exists():
            import shutil
            dest_file = output_dir / source_file.name
            shutil.move(str(source_file), str(dest_file))
            print(f"✓ Moved {source_file.name} to {config_hash}/")
    else:
        print("\n⊘ Skipping histogram generation (--skip-histograms)")
    
    # Step 2: Generate plots
    if not args.skip_plots:
        # Check if the specific expected .coffea file exists
        if not expected_coffea_path.exists():
            print(f"\n✗ Error: Expected .coffea file not found: {expected_coffea_path}")
            sys.exit(1)
        
        print(f"\nProcessing: {expected_coffea}")
        
        coffea_file = expected_coffea_path
            
        # Create plots subdirectory in hash output dir (shared for both analysis types)
        plots_dir = output_dir / 'plots'
        plots_dir.mkdir(exist_ok=True)
        
        cmd = [
            sys.executable,
            f"scripts/{plotter_script}",  # Path relative to 004-Reconstruction
            str(coffea_file),
            '--output-dir', str(plots_dir),
        ]
        
        run_command(cmd, f"Step 2: Generate plots ({plotter_script})")
        
        print(f"\n✓ All plots saved to: {plots_dir}")
    else:
        print("\n⊘ Skipping plot generation (--skip-plots)")
    
    # Log this run
    run_history = Path(__file__).parent.parent / 'run_history.txt'
    utils.log_run(run_history, config_hash, {
        'analysis_type': analysis_type,
        'era': era,
        'tag': tag,
        'variables': variables,
        'apply_chi2_filter': apply_chi2_filter
    })
    print(f"\n✓ Logged run to run_history.txt")
    
    # Summary for this era and analysis type
    print(f"\n{'='*80}")
    print(f"SUMMARY for {analysis_type.upper()} - {era}")
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
