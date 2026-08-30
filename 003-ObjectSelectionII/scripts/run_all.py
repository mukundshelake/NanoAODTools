#!/usr/bin/env python3
"""
Master script to generate all outputs for 003-ObjectSelectionII chapter.

Usage:
    python scripts/run_all.py [--force] [--tag TAG_NAME]
    
Options:
    --force: Regenerate outputs even if config hash already exists
    --tag: Create a named tag symlink to this run (e.g., "baseline", "paper_v1")
"""

import argparse
import gzip
import os
import shutil
import sys
from pathlib import Path
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
import utils


def matches_filter(filters, era, data_mc=None, group=None, dataset=None):
    """Check if era/DataMC/group/dataset matches any of the provided filters.

    Each filter is a slash-separated string, e.g. 'UL2017/MC_mu/SingleTop/Tchannel'.
    Use '*' as a wildcard for any level.
    A shorter filter path matches all entries at deeper levels.
    """
    if not filters:
        return True
    for f in filters:
        parts = f.split('/')
        if parts[0] not in ('*', era):
            continue
        if data_mc is not None and len(parts) >= 2 and parts[1] not in ('*', data_mc):
            continue
        if group is not None and len(parts) >= 3 and parts[2] not in ('*', group):
            continue
        if dataset is not None and len(parts) >= 4 and parts[3] not in ('*', dataset):
            continue
        return True
    return False


def main(): 
    parser = argparse.ArgumentParser(description='Generate all outputs for 003-ObjectSelectionII')
    parser.add_argument('-t', '--tag', type=str,
                       help='Create named tag for this run (e.g., baseline, paper_v1)', default='Dump')
    parser.add_argument('--force', action='store_true',
                       help='Regenerate outputs even if output files already exist for this config hash')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--generateSelectionIDatasetJSON', action='store_true',
                       help='[0] Scan {STORAGE}/selectionI/{selectionITag}/{selectionIHash}/{era} on disk '
                            '(the 003-ObjectSelectionI output) and build inputs/selectionI_{era}_datasets.json '
                            'via scripts/generateDatasetJSON.py -- the same health-checked scan 003-ObjectSelectionI '
                            'itself uses, run fresh each time so the recorded file paths always reflect where the '
                            'files actually are right now (also copied into this run\'s outputs/{tag}/{hash}/inputs/ '
                            'snapshot). Requires --selectionITag and --selectionIHash.')
    parser.add_argument('--selectionITag', type=str, default=None,
                       help='[0] Tag of the 003-ObjectSelectionI run to scan (e.g. "midAugust"). '
                            'Required by --generateSelectionIDatasetJSON.')
    parser.add_argument('--selectionIHash', type=str, default=None,
                       help='[0] Config hash of the 003-ObjectSelectionI run to scan. '
                            'Required by --generateSelectionIDatasetJSON.')
    parser.add_argument('--downloadGoldenJSONs', action='store_true',
                       help='[0] Download {era}_goldenJSON.json for each era directly from the CMS URLs in '
                            'config.yaml\'s golden_json_urls, into inputs/ (and this run\'s outputs/{tag}/{hash}/'
                            'inputs/ snapshot). Independent of any particular 003-ObjectSelectionI run -- the '
                            'golden JSON only depends on era, not on a selectionI tag/hash.')
    parser.add_argument('--fetchSFFiles', action='store_true',
                       help='[0a] Fetch correctionlib SF files (muon ID/HLT, jet PU ID, b-tagging) from '
                            "SFSource (hostname-resolved, e.g. lxplus's CVMFS jsonpog-integration mount) into "
                            'inputs/SFs/. Idempotent: a file already present in inputs/ is left alone and not '
                            're-fetched -- pass --force to refetch everything regardless.')
    parser.add_argument('--prepareEfficiencyFileset', action='store_true',
                       help='[0b] Build a per-era, MC-only coffea fileset JSON from the selectionI dataset JSON '
                            '(built via --generateSelectionIDatasetJSON) for --computeJetPUIDEfficiency / '
                            '--computeBTaggingEfficiency. Uses selectionI (pre-weight) skims, not this '
                            "chapter's own output, since the efficiency maps are needed by the weight "
                            'modules themselves.')
    parser.add_argument('--computeJetPUIDEfficiency', action='store_true',
                       help='[0c] Compute Jet PU ID efficiency maps (scripts/computeJetPUIDEfficiency.py) from '
                            'the fileset(s) built by --prepareEfficiencyFileset, writing ROOT files into '
                            "inputs/SFs/JetPUID/Efficiency/<era>/<sample>.root (config.yaml's "
                            "jetPUID.efficiencyFolder) and copying them into this run's own "
                            "outputs/{tag}/{hash}/inputs/SFs/JetPUID/Efficiency/<era>/ snapshot.")
    parser.add_argument('--computeBTaggingEfficiency', action='store_true',
                       help='[0d] Compute per-flavor b-tagging efficiency maps (scripts/computeBTaggingEfficiency.py) '
                            'from the fileset(s) built by --prepareEfficiencyFileset, writing ROOT files into '
                            "inputs/SFs/Efficiency/<era>/<sample>.root (config.yaml's "
                            "bTagging.efficiencyFolder) and copying them into this run's own "
                            "outputs/{tag}/{hash}/inputs/SFs/Efficiency/<era>/ snapshot.")
    parser.add_argument('--generateProcessListJSON', action='store_true',
                       help='[1] Generate process list JSON for runSelection.py by reading the per-era '
                            'dataset JSONs produced by --generateDatasetJSON')
    parser.add_argument('--writeBashScript', action='store_true',
                       help='[2] Write a bash script with all runSelection.py commands instead of executing them directly')
    parser.add_argument('--runBashScript', action='store_true',
                       help='[2b] Execute the run_all_<tag>.sh script written by --writeBashScript, streaming '
                            'its output live. Can be combined with --writeBashScript in the same invocation '
                            '(write then run), or run alone against a script written by an earlier invocation.')
    parser.add_argument('--submitSelectionJobs', action='store_true',
                       help='[2alt][lxplus][CRAB] Submit scale-factor-weight jobs to CRAB instead of running them '
                            'locally -- an alternative to --writeBashScript + local execution. Processes the same '
                            'selectionI skims (via Data.userInputFiles, since they are not DBS-registered) and '
                            'writes output to the same {STORAGE}/selectionII/{tag}/{hash}/... layout, so downstream '
                            'steps work unchanged either way. Uses scripts/crab/submit_selectionII_flexible.py.')
    parser.add_argument('--checkCrabStatus', action='store_true',
                       help='[lxplus][CRAB] Check CRAB job status for jobs submitted with --submitSelectionJobs. '
                            'Uses scripts/crab/checkStatus.py.')
    parser.add_argument('--resubmitFailedCrabJobs', action='store_true',
                       help='[lxplus][CRAB] With --checkCrabStatus: resubmit failed CRAB jobs.')
    parser.add_argument('--removeSubmitFailedCrabJobs', action='store_true',
                       help='[lxplus][CRAB] With --checkCrabStatus: remove CRAB jobs that never submitted successfully.')
    parser.add_argument('--generateDatasetJSON', action='store_true',
                       help='[3] Generate dataset JSON file using the script generateDatasetJSON.py')
    parser.add_argument('--computeABCDScaleFactor', action='store_true',
                       help='[3b] Run scripts/computeABCDScaleFactor.py on selectionII_{tag}_{era}_datasets.json '
                            '(from --generateDatasetJSON): computes the data-driven QCD transfer factor '
                            'R = N_C/N_D, binned in (SelMuon_pt, |SelMuon_eta|). Runs after the SF-weight skims '
                            'exist, so the background subtraction underneath R uses the same full per-event '
                            'weight (muonIDWeight, muonHLTWeight, bTagWeight, L1PreFiringWeight_Nom, '
                            'lheWeightSign, on top of Lumi*Xsec/Ngen) as the rest of the analysis -- unlike the '
                            'old 003-ObjectSelectionI version of this script, which could only use '
                            'Lumi*Xsec/Ngen*sign(LHEWeight) since the SF branches did not exist yet at that '
                            'stage. Writes a ROOT file of TH2 maps and a JSON report per era to '
                            'outputs/{tag}/{hash}/{era}/. 003-ObjectSelectionIII fetches this file directly from '
                            "here and looks up R live, per event, when building region-B histograms -- there is "
                            'no per-event branch/module for it in this chapter.')
    parser.add_argument('--prepareFileset', action='store_true',
                       help='[4] Prepare the fileset for coffea processing.')
    parser.add_argument('--printHash', action='store_true',
                       help='Print config hash and exit (for testing purposes)')
    parser.add_argument('--sample', action='store_true',
                       help='Only add the first file of each dataset to the process list JSON (for testing purposes)')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of parallel workers passed to runSelection.py (default: 15)')
    args = parser.parse_args()

    # parsing arguments
    print("Arguments:")
    print(f"  --tag: {args.tag}")
    print(f"  --generateSelectionIDatasetJSON: {args.generateSelectionIDatasetJSON}")
    print(f"  --selectionITag: {args.selectionITag}")
    print(f"  --selectionIHash: {args.selectionIHash}")
    print(f"  --downloadGoldenJSONs: {args.downloadGoldenJSONs}")
    print(f"  --fetchSFFiles: {args.fetchSFFiles}")
    print(f"  --prepareEfficiencyFileset: {args.prepareEfficiencyFileset}")
    print(f"  --computeJetPUIDEfficiency: {args.computeJetPUIDEfficiency}")
    print(f"  --computeBTaggingEfficiency: {args.computeBTaggingEfficiency}")
    print(f"  --generateProcessListJSON: {args.generateProcessListJSON}")
    print(f"  --writeBashScript: {args.writeBashScript}")
    print(f"  --runBashScript: {args.runBashScript}")
    print(f"  --submitSelectionJobs: {args.submitSelectionJobs}")
    print(f"  --checkCrabStatus: {args.checkCrabStatus}")
    print(f"  --resubmitFailedCrabJobs: {args.resubmitFailedCrabJobs}")
    print(f"  --removeSubmitFailedCrabJobs: {args.removeSubmitFailedCrabJobs}")
    print(f"  --generateDatasetJSON: {args.generateDatasetJSON}")
    print(f"  --computeABCDScaleFactor: {args.computeABCDScaleFactor}")
    print(f"  --prepareFileset: {args.prepareFileset}")
    print(f"  --sample: {args.sample}")
    print(f"  --workers: {args.workers}")
    print(f"  --force: {args.force}")
    print(f"  --filter: {args.filter}")
    print(f"  --printHash: {args.printHash}")

    # Paths
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / 'config.yaml'
    outputs_base = base_dir / 'outputs' / f'{args.tag}'
    inputs_folder = base_dir / 'inputs'

    print(f"Using config: {config_path}")

    # Load config and compute hash
    config = utils.load_config(config_path)

    # Create output directory
    output_dir, config_hash, is_new_run = utils.create_output_directory(
        outputs_base, config_path, inputs_folder
    )
    if is_new_run:
        print(f"Config file has changed. Created new output directory: {output_dir}")
    else:
        print(f"No changes in config. Output directory already exists: {output_dir}")

    # Create or update the 'latest' symlink to point to the current output directory
    latest_link = outputs_base / 'latest'
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(output_dir.name)
    print(f"Updated symlink: {latest_link} -> {output_dir.name}")

    storageBase = utils.resolve_storage_path(config)
    print(f"Using storage base: {storageBase}")

    if args.printHash:
        print(f"Config hash: {config_hash}")
        return 0

    # Scan 003-ObjectSelectionI's selectionI ROOT files on disk and build
    # inputs/selectionI_{era}_datasets.json fresh via generateDatasetJSON.py --
    # replaces the old --fetchFromPreviousChapter, which just copied a JSON that
    # 003-ObjectSelectionI had generated at some earlier point and could go stale (its
    # recorded paths pointing at files that had since moved or become
    # unreachable from this machine, with nothing to catch the drift).
    if args.generateSelectionIDatasetJSON:
        if not args.selectionITag or not args.selectionIHash:
            print("Error: --generateSelectionIDatasetJSON requires --selectionITag and --selectionIHash.")
            return 1
        print(f"\nGenerating selectionI dataset JSON from disk (tag: {args.selectionITag}, "
              f"hash: {args.selectionIHash})...")
        generateJSON_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        if not generateJSON_script.exists():
            print(f"Error: {generateJSON_script} not found!")
            return 1
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            output_json_name = f"selectionI_{era}_datasets.json"
            base_directory = os.path.join(storageBase, "selectionI", args.selectionITag,
                                           args.selectionIHash, era)
            cmd = [
                sys.executable, str(generateJSON_script),
                '--outputDirectory', str(inputs_folder),
                '--outputFileName', output_json_name,
                '--baseDirectory', base_directory,
            ]
            print(f"    Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"    Error running generateDatasetJSON.py for era {era}:\n{result.stderr}")
                return 1
            # Also copy into this run's own outputs/{tag}/{hash}/inputs/ snapshot --
            # create_output_directory() only snapshotted inputs/ as it existed at the
            # start of this invocation, before this step ran.
            output_path = output_dir / 'inputs' / output_json_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(inputs_folder / output_json_name, output_path)
            print(f"    Generated {inputs_folder / output_json_name} and copied to {output_path}")
        print("Finished generating selectionI dataset JSON files.")

    # Download {era}_goldenJSON.json directly from the CMS URLs in config.yaml's
    # golden_json_urls. Independent of any particular 003-ObjectSelectionI run -- the
    # golden JSON content only depends on era.
    if args.downloadGoldenJSONs:
        print("\nDownloading golden JSON files specified in config...")
        download_script = base_dir / 'scripts' / 'downloadGoldenJsons.py'
        if not download_script.exists():
            print(f"Error: {download_script} not found!")
            return 1
        golden_json_urls = config.get('golden_json_urls', {})
        if not golden_json_urls:
            print("Error: config.yaml has no golden_json_urls section.")
            return 1
        for era, url in golden_json_urls.items():
            if not matches_filter(args.filter, era):
                continue
            print(f"  Era: {era}")
            output_filename = f"{era}_goldenJSON.json"
            local_path = inputs_folder / output_filename
            if local_path.exists() and not args.force:
                print(f"    Already exists and --force not specified, skipping download: {local_path}")
            else:
                cmd = [sys.executable, str(download_script), '-u', url,
                       '-o', output_filename, '-outDir', str(inputs_folder)]
                print(f"    Running command: {' '.join(cmd)}")
                result = subprocess.run(cmd)
                if result.returncode != 0:
                    print(f"    Error downloading golden JSON for era {era}")
                    return 1
            output_path = output_dir / 'inputs' / output_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, output_path)
            print(f"    {local_path} copied to {output_path}")
        print("Finished downloading golden JSON files.")

    # Fetch correctionlib SF files from CVMFS (or wherever SFSource resolves to on this
    # machine) into inputs/SFs/. Treated as an input like the selectionI dataset JSON
    # above: idempotent (skip a file already present locally, unless --force), and
    # dual-written into both the persistent inputs_folder and this run's own
    # output_dir/inputs snapshot for the same reason as above -- the snapshot copy at
    # the top of this script only reflects inputs_folder as it was before this
    # invocation's own fetch runs.
    if args.fetchSFFiles:
        print("\nFetching correctionlib SF files...")
        sf_source_base, ssh_relay_host = utils.resolve_sf_source(config)
        sf_source_base = Path(sf_source_base)
        if ssh_relay_host:
            print(f"Using SF source base: {sf_source_base} (relayed over SSH via {ssh_relay_host})")
            print(f"  This machine has no direct SFSource entry -- may prompt for your "
                  f"password/2FA on first connection to {ssh_relay_host}; respond in this terminal.")
        else:
            print(f"Using SF source base: {sf_source_base}")
        any_fetched = False
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            era_dir = utils.cvmfs_era_dir(era)
            for spec in utils.SF_FETCH_SPECS:
                source_path = sf_source_base / spec['pog'] / era_dir / spec['source_filename']
                for out_suffix in spec['outputs']:
                    rel_path = Path('SFs') / f"{era}_{out_suffix}"
                    local_path = inputs_folder / rel_path
                    snapshot_path = output_dir / 'inputs' / rel_path
                    if local_path.exists() and not args.force:
                        print(f"  [skip, already fetched] {rel_path}")
                        continue
                    if ssh_relay_host:
                        try:
                            raw = utils.ssh_read_file(ssh_relay_host, source_path)
                        except FileNotFoundError as e:
                            print(f"  Error: {e}. Skipping {rel_path}.")
                            continue
                    else:
                        if not source_path.exists():
                            print(f"  Error: source not found: {source_path}. Skipping {rel_path}.")
                            continue
                        raw = source_path.read_bytes()
                    content = gzip.decompress(raw) if spec['gunzip'] else raw
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    local_path.write_bytes(content)
                    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(local_path, snapshot_path)
                    any_fetched = True
                    src_desc = f"{ssh_relay_host}:{source_path}" if ssh_relay_host else str(source_path)
                    print(f"  Fetched {src_desc} -> {local_path}")
        if not any_fetched:
            print("  All SF files already present in inputs/SFs/ (use --force to refetch).")

    # Build a per-era, MC-only coffea fileset from the selectionI (pre-weight) skims,
    # for the two efficiency-map computers below. Deliberately sourced from selectionI's
    # dataset JSON, not this chapter's own (selectionII) output: the efficiency maps are
    # an input the weight modules need, so they can't depend on this chapter having
    # already run with those weights applied.
    if args.prepareEfficiencyFileset:
        print("\nPreparing MC-only efficiency fileset(s) from selectionI inputs...")
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            selectionI_dataset_json = output_dir / 'inputs' / f'selectionI_{era}_datasets.json'
            if not selectionI_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found for era {era}: {selectionI_dataset_json}. "
                      f"Run --generateSelectionIDatasetJSON first. Skipping.")
                continue
            with open(selectionI_dataset_json) as f:
                datasetJSON = json.load(f)

            fileset = {}
            for DataMC in datasetJSON:
                if DataMC.lower().startswith("data"):
                    continue  # efficiency maps are an MC-truth quantity (hadronFlavour, genuine PU-ID pass)
                if not matches_filter(args.filter, era, DataMC):
                    continue
                for group in datasetJSON[DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        files = datasetJSON[DataMC][group][dataset]
                        if not files:
                            print(f"  Warning: {era}/{DataMC}/{group}/{dataset} has no files in the "
                                  f"selectionI dataset JSON. Skipping.")
                            continue
                        datasetName = f'{era}_{DataMC}_{group}_{dataset}'
                        fileset[datasetName] = {
                            "files": files,
                            "metadata": {"isData": False, "era": era, "sample": dataset},
                        }

            fileset_output_path = output_dir / era / f"{args.tag}_{era}_efficiencyFileset.json"
            fileset_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(fileset_output_path, 'w') as f:
                json.dump(fileset, f, indent=2)
            print(f"  Era {era}: {len(fileset)} MC dataset(s) -> {fileset_output_path}")

    # Compute Jet PU ID efficiency maps into the chapter-local inputs/SFs/ folder --
    # the same location --fetchSFFiles uses for every other SF input, and what
    # config.yaml's jetPUID.efficiencyFolder now points to (no longer the shared,
    # repo-root SFs/ folder). Also copied into this run's own
    # outputs/{tag}/{hash}/inputs/SFs/ snapshot, same as every other fetch/compute step.
    if args.computeJetPUIDEfficiency:
        print("\nComputing Jet PU ID efficiency maps...")
        compute_script = base_dir / 'scripts' / 'computeJetPUIDEfficiency.py'
        jetpuid_outdir = inputs_folder / 'SFs' / 'JetPUID' / 'Efficiency'
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            fileset_path = output_dir / era / f"{args.tag}_{era}_efficiencyFileset.json"
            if not fileset_path.exists():
                print(f"  Warning: Efficiency fileset not found for era {era}: {fileset_path}. "
                      f"Run --prepareEfficiencyFileset first. Skipping.")
                continue
            cmd = [
                sys.executable, str(compute_script),
                '--fileList', str(fileset_path),
                '--outputDir', str(jetpuid_outdir),
                '--workers', str(args.workers),
            ]
            if args.sample:
                cmd.append('--sample')
            print(f"  Era {era}: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"Error computing Jet PU ID efficiency for era {era}")
                return 1
            era_outdir = jetpuid_outdir / era
            if era_outdir.exists():
                snapshot_dir = output_dir / 'inputs' / 'SFs' / 'JetPUID' / 'Efficiency' / era
                if snapshot_dir.exists():
                    shutil.rmtree(snapshot_dir)
                shutil.copytree(era_outdir, snapshot_dir)
                print(f"  Era {era}: copied {era_outdir} -> {snapshot_dir}")
        print(f"Jet PU ID efficiency maps written under: {jetpuid_outdir}")

    # Compute per-flavor b-tagging efficiency maps into the chapter-local inputs/SFs/
    # folder, same reasoning as --computeJetPUIDEfficiency above.
    if args.computeBTaggingEfficiency:
        print("\nComputing b-tagging efficiency maps...")
        compute_script = base_dir / 'scripts' / 'computeBTaggingEfficiency.py'
        btag_outdir = inputs_folder / 'SFs' / 'Efficiency'
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            fileset_path = output_dir / era / f"{args.tag}_{era}_efficiencyFileset.json"
            if not fileset_path.exists():
                print(f"  Warning: Efficiency fileset not found for era {era}: {fileset_path}. "
                      f"Run --prepareEfficiencyFileset first. Skipping.")
                continue
            # bTagSFFile in config.yaml is relative to output_dir (e.g. "inputs/SFs/..."),
            # same as it resolves for the module itself once runSelectionII.py chdirs there.
            bTagSFFile = output_dir / config['Modules']['bTagging'][era]['bTagSFFile']
            if not bTagSFFile.exists():
                print(f"  Warning: bTagSFFile not found for era {era}: {bTagSFFile}. "
                      f"Run --fetchSFFiles first. Skipping.")
                continue
            cmd = [
                sys.executable, str(compute_script),
                '--fileList', str(fileset_path),
                '--outputDir', str(btag_outdir),
                '--bTagSFFile', str(bTagSFFile),
                '--workers', str(args.workers),
            ]
            if args.sample:
                cmd.append('--sample')
            print(f"  Era {era}: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"Error computing b-tagging efficiency for era {era}")
                return 1
            era_outdir = btag_outdir / era
            if era_outdir.exists():
                snapshot_dir = output_dir / 'inputs' / 'SFs' / 'Efficiency' / era
                if snapshot_dir.exists():
                    shutil.rmtree(snapshot_dir)
                shutil.copytree(era_outdir, snapshot_dir)
                print(f"  Era {era}: copied {era_outdir} -> {snapshot_dir}")
        print(f"b-tagging efficiency maps written under: {btag_outdir}")

    # Generate process list JSON for runSelection.py
    if args.generateProcessListJSON:
        print("\nGenerating process list JSON for runSelection.py...")
        # NanoAODTools root is one level above 003-ObjectSelection
        nanoaodtools_base = base_dir.parent
        total_tasks = 0

        for era in config['NgenandXsec']:
            print(f"\nProcessing era: {era}")
            if not matches_filter(args.filter, era):
                continue
            selectionI_dataset_json = output_dir / 'inputs' / f'selectionI_{era}_datasets.json'
            golden_json_file = output_dir / 'inputs' / f'{era}_goldenJSON.json'

            if not selectionI_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found: {selectionI_dataset_json}. Skipping era {era}.")
                continue
            if not golden_json_file.exists():
                print(f"  Warning: Golden JSON file not found: {golden_json_file}. Data tasks will run without golden JSON filtering for era {era}.")


            with open(selectionI_dataset_json) as f:
                datasetJSON = json.load(f)

            # Build combined cut string for this era
            era_cuts = config.get('SelectionCuts', {}).get(era, {})
            cut_string = " && ".join(v for v in era_cuts.values() if v and v.strip()) or None

            era_process_list = []
            era_skipped = 0
            for DataMC in datasetJSON:
                if not matches_filter(args.filter, era, DataMC):
                    continue
                print(f"  Processing {era} / {DataMC}...")
                is_data = DataMC.lower().startswith("data")
                modules_key = "Data" if is_data else "MC"
                module_names = config.get("ModuleList", {}).get(modules_key, [])
                print(f"  Processing {era} / {DataMC} / with modules: {module_names}")
                # continue
                # continue
                for group in datasetJSON[DataMC]:
                    if not matches_filter(args.filter, era, DataMC, group):
                        continue
                    print(f"  Processing {era} / {DataMC} / {group}...")
                    # continue
                    for dataset in datasetJSON[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        print (f"  Processing {era} / {DataMC} / {group} / {dataset}...")
                        # continue
                        # print(storageBase, args.tag, era, DataMC, group, dataset)
                        outputDir = os.path.join(
                            storageBase, "selectionII", args.tag, config_hash, era, DataMC, group, dataset
                        )
                        isSample = True
                        for filePath in datasetJSON[DataMC][group][dataset]:
                            
                            # print(f"    Found file: {filePath}")
                            # continue

                            # Build module configs: era-resolved + absolute SF paths



                            module_configs = []
                            for mod_name in module_names:
                                mod_cfg_raw = config.get("Modules", {}).get(mod_name, {})
                                # Use era-specific sub-config if present, else use top-level config
                                mod_cfg = mod_cfg_raw.get(era, mod_cfg_raw)
                                if mod_name == "selectedObjects":
                                    mod_cfg = dict(mod_cfg, is_mc=not is_data)
                                module_configs.append({"name": mod_name, "config": mod_cfg})
                            task = {
                                "era":       era,
                                "DataMC":    DataMC,
                                "group":     group,
                                "dataset":   dataset,
                                "outputDir": outputDir,
                                "file":      filePath,
                                "cut_string": cut_string,
                                "goldenJSON": str(golden_json_file) if is_data else None,
                                "branchsel": None,
                                "modules":   module_configs,
                                "isSample": isSample
                            }
                            era_process_list.append(task)
                            isSample = False  # Only the first file of each dataset is added when --sample is used
            era_output_path = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
            era_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(era_output_path, 'w') as f:
                json.dump(era_process_list, f, indent=2)
            # print(f"  Era {era}: {len(era_process_list)} tasks queued, {era_skipped} already done -> {era_output_path}")
            total_tasks += len(era_process_list)


        print(f"\nTotal tasks across all eras: {total_tasks}")
    
    # If --writeBashScript is set, write all runSelection.py commands to a bash script instead of executing them
    if args.writeBashScript:
        # place the script in the scripts folder itself (where this run_all.py is located)
        bash_script_path = base_dir / 'scripts' / f"run_all_{args.tag}.sh"
        with open(bash_script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            for era in config['NgenandXsec']:
                if not matches_filter(args.filter, era):
                    continue
                process_list_json = output_dir / era / f"{args.tag}_{era}_processListJSON.json"
                if not process_list_json.exists():
                    print(f"  Warning: Process list JSON not found for era {era}: {process_list_json}. Skipping runSelection command for this era.")
                    continue
                for DataMC in config['NgenandXsec'][era]:
                    if not matches_filter(args.filter, era, DataMC):
                        continue
                    for group in config['NgenandXsec'][era][DataMC]:
                        if not matches_filter(args.filter, era, DataMC, group):
                            continue
                        log_path = output_dir / era / DataMC / group / f"{args.tag}_{era}_{DataMC}_{group}.log"
                        log_path.parent.mkdir(parents=True, exist_ok=True)
                        cmd = (
                            f"python3 {base_dir / 'scripts' / 'runSelectionII.py'} "
                            f"--processListJSON {process_list_json} "
                            f"--workers {args.workers} "
                            f"{'--force ' if args.force else ''}"
                            f"{'--sample ' if args.sample else ''}"
                            f"{'--filter ' + era + '/' + DataMC + '/' + group}"
                            f"{' 2>&1 | tee -a ' + str(log_path)}"
                        )
                        f.write(cmd + "\n")
        os.chmod(bash_script_path, 0o755)
        print(f"\nBash script with runSelectionII.py commands written to: {bash_script_path}")

    # Execute the bash script written by --writeBashScript (this invocation's, or an
    # earlier one's -- the path is deterministic from --tag alone). Streamed live, not
    # captured, since this is the long-running processing step.
    if args.runBashScript:
        bash_script_path = base_dir / 'scripts' / f"run_all_{args.tag}.sh"
        if not bash_script_path.exists():
            print(f"Error: Bash script not found: {bash_script_path}. Run with --writeBashScript first.")
            return 1
        print(f"\nExecuting bash script: {bash_script_path}")
        result = subprocess.run(["bash", str(bash_script_path)])
        if result.returncode != 0:
            print(f"Error: {bash_script_path} exited with code {result.returncode}")
            return 1
        print(f"Finished executing: {bash_script_path}")

    # Submit scale-factor-weight jobs to CRAB, as an alternative to --writeBashScript +
    # local execution. lxplus only (needs STORAGE to resolve to the EOS mount of LFN_Base).
    if args.submitSelectionJobs:
        print("\nSubmitting scale-factor-weight jobs to CRAB...")
        submit_selectionII_script = base_dir / 'scripts' / 'crab' / 'submit_selectionII_flexible.py'
        if not submit_selectionII_script.exists():
            print(f"Error: {submit_selectionII_script} not found!")
            return 1
        lfn_base = config.get('LFN_Base', '').rstrip('/')
        if not lfn_base:
            print("Error: LFN_Base not set in config.yaml; required for --submitSelectionJobs.")
            return 1
        # Phase 1: build the flat task list (one CRAB submission per dataset),
        # enumerating from the actual fetched selectionI dataset JSON, not
        # config['NgenandXsec'] -- a hand-maintained table that can drift from
        # what selectionI actually produced, silently dropping coverage here
        # with no warning.
        tasks = []  # (label, command, work_area)
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / 'inputs' / f'selectionI_{era}_datasets.json'
            if not dataset_json_path.exists():
                print(f"Error: Dataset JSON not found for era {era} at {dataset_json_path}. Run --generateSelectionIDatasetJSON first.")
                continue
            golden_json_path = output_dir / 'inputs' / f'{era}_goldenJSON.json'
            with open(dataset_json_path) as jf:
                era_dataset_json = json.load(jf)
            for DataMC in era_dataset_json:
                for group in era_dataset_json[DataMC]:
                    for dataset in era_dataset_json[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        work_area = output_dir / era / DataMC / group / dataset / "crab_selection"
                        # Idempotency: CRAB refuses to submit into an existing requestName
                        # directory ("Working area already exists"). Skip datasets already
                        # submitted unless --force.
                        if work_area.exists() and any(work_area.glob('crab_selII_*')) and not args.force:
                            print(f"  Already submitted (work area exists), skipping: {work_area}")
                            continue
                        lfn_output_path = f"{lfn_base}/selectionII/{args.tag}/{config_hash}/{era}/{DataMC}/{group}/{dataset}"
                        command = (
                            f"python3 {submit_selectionII_script} --submit --era {era} "
                            f"--dataset-json {dataset_json_path} --golden-json {golden_json_path} "
                            f"--output-lfn {lfn_output_path} --work-area {work_area} "
                            f"{'--sample ' if args.sample else ''}"
                            f"--include '{DataMC}/{group}/{dataset}'"
                        )
                        tasks.append((f"{era}/{DataMC}/{group}/{dataset}", command, work_area))

        print(f"\n{len(tasks)} datasets to submit. Submitting with {args.workers} parallel workers...")

        # Phase 2: submit in parallel -- CRAB submission is dominated by network
        # round-trip time to cmsweb.cern.ch, same reasoning as 002-Samples'
        # --submitPreSelectionJobs parallelization. Each task is a separate OS
        # process, so there's no concern about the CRAB client's thread-safety.
        submitted, failed = 0, 0
        if tasks:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(subprocess.run, command, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True): (label, command, work_area)
                    for label, command, work_area in tasks
                }
                for i, future in enumerate(as_completed(futures), start=1):
                    label, command, work_area = futures[future]
                    try:
                        result = future.result()
                        ok = (result.returncode == 0)
                        output = result.stdout
                    except Exception as e:
                        ok = False
                        output = str(e)
                    if ok:
                        submitted += 1
                        print(f"  [{i}/{len(tasks)}] Submitted: {label} (logs: {work_area})")
                    else:
                        failed += 1
                        print(f"  [{i}/{len(tasks)}] FAILED: {label}\n{output}")

        print(f"\nsubmitSelectionJobs: {submitted} submitted, {failed} failed out of {len(tasks)} total.")

    # Check CRAB job status for jobs submitted with --submitSelectionJobs
    if args.checkCrabStatus:
        print("\nChecking CRAB job status for scale-factor-weight jobs submitted with --submitSelectionJobs...")
        check_crab_status_script = base_dir / 'scripts' / 'crab' / 'checkStatus.py'
        if not check_crab_status_script.exists():
            print(f"Error: {check_crab_status_script} not found!")
            return 1

        # Phase 1: build the flat task list (one `crab status` check per dataset).
        tasks = []  # (label, command)
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / 'inputs' / f'selectionI_{era}_datasets.json'
            if not dataset_json_path.exists():
                print(f"  Warning: selectionI dataset JSON not found for era {era}: {dataset_json_path}. Skipping.")
                continue
            with open(dataset_json_path) as jf:
                era_dataset_json = json.load(jf)
            for DataMC in era_dataset_json:
                for group in era_dataset_json[DataMC]:
                    for dataset in era_dataset_json[DataMC][group]:
                        if not matches_filter(args.filter, era, DataMC, group, dataset):
                            continue
                        work_area = output_dir / era / DataMC / group / dataset / "crab_selection"
                        command = f"python3 {check_crab_status_script} -d {work_area}"
                        if args.resubmitFailedCrabJobs:
                            command += " --resubmit"
                        if args.removeSubmitFailedCrabJobs:
                            command += " --removeSubmitFailed"
                        tasks.append((f"{era}/{DataMC}/{group}/{dataset}", command))

        print(f"\n{len(tasks)} datasets to check. Checking with {args.workers} parallel workers...")

        # Phase 2: check in parallel -- stdout/stderr are captured (not
        # inherited) so concurrent checks never interleave their output; each
        # dataset's full captured output is printed as one block from this
        # single main-thread loop only after its subprocess finishes.
        succeeded, failed = 0, 0
        if tasks:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(subprocess.run, command, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True): (label, command)
                    for label, command in tasks
                }
                for i, future in enumerate(as_completed(futures), start=1):
                    label, command = futures[future]
                    try:
                        result = future.result()
                        ok = (result.returncode == 0)
                        output = result.stdout
                    except Exception as e:
                        ok = False
                        output = str(e)
                    header = f"[{i}/{len(tasks)}] {label}"
                    print(f"\n{'=' * len(header)}\n{header}\n{'=' * len(header)}\n{output}")
                    if ok:
                        succeeded += 1
                    else:
                        failed += 1
                        print(f"Error checking CRAB status for dataset: {label}")

        print(f"\ncheckCrabStatus: {succeeded} succeeded, {failed} failed out of {len(tasks)} total.")

    if args.generateDatasetJSON:
        print("\nGenerating dataset JSON file using generateDatasetJSON.py...")
        generate_dataset_json_script = base_dir / 'scripts' / 'generateDatasetJSON.py'
        # Check if the script exists
        if not generate_dataset_json_script.exists():
            print(f"Error: Script not found: {generate_dataset_json_script}")
            return 1
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            outputDirectory = output_dir / era 
            outputDirectory.mkdir(parents=True, exist_ok=True)
            outputFileName = f"selectionII_{args.tag}_{era}_datasets.json"
            baseDirectory = os.path.join(storageBase, "selectionII", args.tag, config_hash, era)
            cmd = [
                sys.executable, str(generate_dataset_json_script),
                '--outputDirectory', str(outputDirectory),
                '--outputFileName', outputFileName,
                '--baseDirectory', baseDirectory
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error running generateDatasetJSON.py for era {era}:\n{result.stderr}")
                return 1
            else:
                print(f"Successfully generated dataset JSON for era {era}: {outputDirectory / outputFileName}")

    if args.computeABCDScaleFactor:
        print("\nComputing data-driven ABCD scale factor (scripts/computeABCDScaleFactor.py)...")
        sf_script = base_dir / 'scripts' / 'computeABCDScaleFactor.py'
        if not sf_script.exists():
            print(f"Error: {sf_script} not found!")
            return 1
        sf_failed = False
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            dataset_json_path = output_dir / era / f"selectionII_{args.tag}_{era}_datasets.json"
            if not dataset_json_path.exists():
                print(f"Error: selectionII dataset JSON not found for era {era} at {dataset_json_path}. "
                      f"Run --generateDatasetJSON first.")
                sf_failed = True
                continue
            sf_dir = output_dir / era
            cmd = [
                sys.executable, str(sf_script),
                '--datasetJSON', str(dataset_json_path),
                '--config', str(config_path),
                '--era', era,
                '--outputDir', str(sf_dir),
            ]
            print(f"\nRunning command: {' '.join(cmd)}")
            result = subprocess.run(cmd)
            if result.returncode != 0:
                print(f"computeABCDScaleFactor.py reported problems for era {era}.")
                sf_failed = True
            else:
                print(f"computeABCDScaleFactor.py: output written for era {era} to {sf_dir}")
        if sf_failed:
            return 1

    # Prepare fileset for coffea processing
    if args.prepareFileset:
        print("\nPreparing fileset for coffea processing...")
        for era in config['NgenandXsec']:
            if not matches_filter(args.filter, era):
                continue
            selectionII_dataset_json = output_dir / era / f"selectionII_{args.tag}_{era}_datasets.json"
            if not selectionII_dataset_json.exists():
                print(f"  Warning: Dataset JSON not found for era {era}: {selectionII_dataset_json}. Skipping fileset preparation for this era.")
                continue
            with open(selectionII_dataset_json) as f:
                datasetJSON = json.load(f)
            for DataMC in datasetJSON:
                for group in datasetJSON[DataMC]:
                    for dataset in datasetJSON[DataMC][group]:
                        fileset = {}
                        datasetName = f'{era}_{DataMC}_{group}_{dataset}'
                        fileset[datasetName] = {"files": datasetJSON[DataMC][group][dataset]}
                        fileset[datasetName]['metadata'] = {}
                        if 'data' in DataMC.lower():
                            fileset[datasetName]['metadata']['isData'] = True
                        else:
                            fileset[datasetName]['metadata']['isData'] = False
                            fileset[datasetName]['metadata']['era'] = era
                            fileset[datasetName]['metadata']['sample'] = dataset
                        # Save the fileset as a JSON file for this era
                        fileset_output_path = output_dir / era / DataMC / group / dataset / f"{args.tag}_{DataMC}_{group}_{dataset}_{era}_fileset.json"
                        fileset_output_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(fileset_output_path, 'w') as f:
                            json.dump(fileset, f, indent=4)
                        print(f"Prepared fileset for era {era} and saved to: {fileset_output_path}")
    # exit(0)


if __name__ == '__main__':
    sys.exit(main())
