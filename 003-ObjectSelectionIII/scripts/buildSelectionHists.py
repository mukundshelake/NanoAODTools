# This script takes the fileset and config file. Builds the Hist histograms using coffea and saves the output as .coffea file in the said output directory with the said name.
# Usage: python buildSelectionHists.py --fileset <path to fileset> --config <path to config file> --outputDir <path to output directory> --outputName <name of output file>

import os
import json
from pathlib import Path
import yaml
import argparse
import logging
import numpy as np
import dask
import dask.array as da
import dask_awkward as dak
import dask_histogram as dh
import boost_histogram as bh
from coffea.nanoevents import NanoAODSchema, BaseSchema
from coffea import processor
from coffea.dataset_tools import (
    apply_to_fileset,
    max_chunks,
    preprocess,
)
from coffea.util import save
from coffea.lookup_tools import extractor


# Configure logger
logging.basicConfig(
    level=logging.INFO,  # Use INFO or WARNING in production
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


class WeightLookupProcessor(processor.ProcessorABC):
    def __init__(self, config, region_filter, abcd_scale_factor_file=None):
        self.config = config
        # ABCD_region code: 0=A (signal region), 1=B (QCD control region), 2=C, 3=D.
        # See 003-ObjectSelectionI's SelectedObjectsProducer for the full convention.
        self.region_filter = region_filter

        # ABCD transfer factor R = N_C/N_D, binned in (SelMuon_pt, |SelMuon_eta|),
        # computed by 003-ObjectSelectionII's computeABCDScaleFactor.py. Only needed
        # for the region-B pass -- see process() below. Same coffea extractor lookup
        # mechanism 003-ObjectSelectionII's bTaggingWeight.py uses for its efficiency
        # maps, just evaluated live here instead of pre-baked into a skim branch.
        self.abcd_evaluator = None
        self.abcd_hist_name = "ABCD_transferFactor_R"
        if abcd_scale_factor_file:
            ext = extractor()
            ext.add_weight_sets([f"* * {abcd_scale_factor_file}"])
            ext.finalize()
            self.abcd_evaluator = ext.make_evaluator()

    def process(self, events):
        dataset = events.metadata['dataset']
        isData = events.metadata['isData']
        logger.info(f"Processing dataset: {dataset} (ABCD_region == {self.region_filter})")

        # Scope to one ABCD region. This didn't exist before ABCD tagging: every event
        # used to be implicitly region A/C only (tight isolation was the only option at
        # the event-selection stage), so "no filter" silently meant "region A ∪ C" -- now
        # that 003-ObjectSelectionI admits loose-isolation (B/D) events too, this filter
        # is required to keep the "nominal" (region A) plots meaning what they always meant.
        #
        # IMPORTANT: mask at the dask.array level (convert with dak.to_dask_array() first,
        # *then* compare/index), not at the awkward level (events[events.ABCD_region == x]
        # or events[var][awkward_mask]). Awkward-level boolean masking on this dataset hangs
        # indefinitely in dask.compute() (reproduced directly, independent of scheduler --
        # threads and synchronous both hang; isolated by bisecting a minimal repro script).
        # dask.array-level masking (region_mask below) computes correctly in ~1-2s. Root
        # cause not fully understood (looks like a dask-awkward inefficiency/bug with
        # per-partition-length-changing boolean masks in this environment), but the
        # dask.array workaround is fully equivalent for our purposes (every array here is
        # 1D, event-flat) and reliably fast.
        region_mask = dak.to_dask_array(events.ABCD_region) == self.region_filter

        # Build total weights by multiplying the individual weights together
        total_weights = None
        weightList = self.config['weightList']['Data'] if isData else self.config['weightList']['MC']
        for weight in weightList:
            if weight in events.fields:
                w = dak.to_dask_array(events[weight])[region_mask]
                total_weights = w if total_weights is None else total_weights * w
            else:
                logger.warning(f"Weight '{weight}' not found in events. Skipping this weight.")

        # For the region-B (QCD control) pass only, additionally fold in the ABCD
        # transfer factor R = N_C/N_D, looked up live per event from SelMuon_pt/|eta|.
        # This converts this region-B histogram -- once background-subtracted at the
        # aggregation stage (Data minus non-QCD MC) -- directly into the
        # properly-normalized region-A QCD prediction (N_A_pred = R * N_B): the
        # standard "shape from control region, per-event-reweighted normalization"
        # ABCD technique, needing no separate overall-normalization step afterward.
        # No sentinel check needed here: ABCD_region == 1 (B) only ever gets assigned
        # when a muon was actually selected (SelectedObjectsProducer sets region = -1,
        # never 1, when no muon was found), so SelMuon_pt is never the -1 sentinel for
        # any event landing in this mask.
        if self.region_filter == 1:
            if self.abcd_evaluator is not None and 'SelMuon_pt' in events.fields and 'SelMuon_eta' in events.fields:
                pt_arr  = dak.to_dask_array(events['SelMuon_pt'])[region_mask]
                eta_arr = dak.to_dask_array(events['SelMuon_eta'])[region_mask]
                r_arr = da.map_blocks(
                    lambda pt, eta: self.abcd_evaluator[self.abcd_hist_name](pt, np.abs(eta)),
                    pt_arr, eta_arr, dtype=np.float64,
                )
                total_weights = r_arr if total_weights is None else total_weights * r_arr
            else:
                logger.warning("ABCD scale factor evaluator not available for the region-B pass -- "
                                "run 003-ObjectSelectionII's --computeABCDScaleFactor and pass "
                                "--abcdScaleFactorFile.")

        # Fill histograms lazily using dask_histogram
        hists = {}
        for hist_ in self.config['histDetails']:
            cfg = self.config['histDetails'][hist_]
            var_name = cfg['variable']
            if var_name not in events.fields:
                logger.warning(f"Variable '{var_name}' not found in events. Skipping histogram '{hist_}'.")
                continue
            data = dak.to_dask_array(events[var_name])[region_mask]
            hists[hist_] = dh.factory(
                data,
                axes=[bh.axis.Regular(cfg['bins'], cfg['range'][0], cfg['range'][1],
                                      metadata={'name': cfg['name'], 'label': cfg['label']})],
                storage=bh.storage.Double(),
                weights=total_weights,
            )
        return {
            "nEvents": region_mask.sum(),
            "hists": hists
        }
    def postprocess(self, accumulator):
        return accumulator

def main():
    parser = argparse.ArgumentParser(description="Lookup weights using coffea")
    parser.add_argument('--fileSet', type=str, required=True, help='Path to the coffea compatible file list JSON')
    parser.add_argument('--configFile', type=str, required=True, help='Path to the YAML config file')
    parser.add_argument('--outputDir', type=str, required=True, help='Directory to save the output .coffea file')
    parser.add_argument('--outputFileName', type=str, required=True, help='Name of the output .coffea file')
    parser.add_argument('--regionFilter', type=int, required=True, choices=[0, 1, 2, 3],
                        help='ABCD_region code to scope histograms to: 0=A (signal region, the nominal '
                             'Data/MC plots), 1=B (QCD control region -- used to build the data-driven QCD '
                             'template, not for direct plotting), 2=C, 3=D.')
    parser.add_argument('--abcdScaleFactorFile', type=str, default=None,
                        help='Path to abcdScaleFactor_{era}.root (003-ObjectSelectionII\'s '
                             'computeABCDScaleFactor.py output). Required when --regionFilter 1 -- the ABCD '
                             'transfer factor R gets looked up from this file and folded into the region-B '
                             'weight, per event, by SelMuon pt/|eta|. Ignored for every other region.')
    args = parser.parse_args()

    if args.regionFilter == 1 and not args.abcdScaleFactorFile:
        parser.error("--abcdScaleFactorFile is required when --regionFilter 1.")

    # Load config
    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)

    # Load fileset
    with open(args.fileSet, 'r') as f:
        fileset = json.load(f)

    # Create processor instance
    processor_instance = WeightLookupProcessor(config, args.regionFilter, args.abcdScaleFactorFile)

    # Preprocess and run with dask
    logger.info("Preprocessing fileset...")
    dataset_runnable, dataset_updated = preprocess(
        fileset,
        align_clusters=False,
        files_per_batch=1,
        skip_bad_files=True,
        save_form=False,
    )

    to_compute = apply_to_fileset(
        processor_instance,
        max_chunks(dataset_runnable, 300),
        schemaclass=BaseSchema,
    )

    (output,) = dask.compute(to_compute, scheduler='threads')

    # Save output
    output_path = os.path.join(args.outputDir, args.outputFileName)
    save(output, output_path)
    logger.info(f"Saved output to {output_path}")   


if __name__ == '__main__':
    from multiprocessing import freeze_support
    freeze_support()
    main()