"""
Generate channels.tsv files for BIDS-compliant iEEG datasets.

This script processes Pennsieve datasets to extract channel metadata
and generate properly formatted channels.tsv sidecar files.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

# Set up import paths - local first, then parent for shared package
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir))
sys.path.insert(1, str(_this_dir.parent))

from shared.helpers import (
    load_data, save_data, get_all_datasets, get_dataset_packages,
    get_freq_duration, get_electrode_data, parse_electrode_txt,
    eps_to_penn_epi, penn_epi_to_eps, clean_channel_name,
    get_channel_info, sanitize_group_name
)
from shared.config import OUTPUT_DIR
from config import MASTER_CSV_PATH
from sidecars import ChannelsSidecar


# =============================================================================
# Constants
# =============================================================================

MULTI_DAY_KEYS = ["D01", "D02", "D03", "D04", "D05", "D06", "D07"]
SKIP_DATASETS = {"PennEPI00949"}


# =============================================================================
# Package Classification Helpers
# =============================================================================

def classify_package(pkg_name: str) -> str:
    """
    Classify a package by its file type.

    Returns one of: 'ieeg_json', 'electrodes_csv', 'electrodes_txt', 'mef', or None
    """
    name_lower = pkg_name.lower().strip()

    if name_lower.endswith("implant_ieeg.json"):
        return "ieeg_json"
    elif name_lower == "electrodes2roi_mni.csv":
        return "electrodes_csv"
    elif name_lower == "electrodes.txt":
        return "electrodes_txt"
    elif name_lower.endswith(".mef"):
        return "mef"
    return None


def is_valid_dataset_name(name: str) -> bool:
    """Check if dataset name is a valid EPS/PennEPI dataset."""
    name_lower = name.lower()
    return name_lower.startswith("eps") or name_lower.startswith("pennepi")


def is_deleted(pkg_content: dict) -> bool:
    """Check if package is deleted or deleting."""
    state = pkg_content.get("state", "")
    return state in ("DELETED", "DELETING")


# =============================================================================
# Data Loading Helpers
# =============================================================================

def get_ref_gnd_map(csv_path: str) -> dict:
    """Load EPS → (reference, ground) mapping from master CSV."""
    mapping = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            eps = row.get("EPS Number")
            if eps:
                mapping[eps.strip()] = (
                    row.get("iEEGReference", "").strip() or "unknown",
                    row.get("iEEGGround", "").strip() or "unknown",
                )
    return mapping


def load_ieeg_metadata(node_id: str, force_reload: bool = False) -> dict:
    """Load sampling frequency and duration from ieeg.json file."""
    cache_key = f"ieeg_json_data_{node_id}"
    data = load_data(cache_key, force_reload=force_reload)

    if data is None:
        print(f"  Fetching ieeg.json metadata...")
        data = get_freq_duration(node_id)
        save_data(data, cache_key)

    return data


def load_electrode_csv(node_id: str, dataset_name: str, force_reload: bool = False) -> list:
    """Load electrode data from CSV file."""
    cache_key = f"electrode_data_{dataset_name}"
    data = load_data(cache_key, force_reload=force_reload)

    if data is None:
        print(f"  Fetching electrode CSV data...")
        data = get_electrode_data(node_id)
        save_data(data, cache_key)

    return data


def load_electrode_txt(node_id: str, dataset_name: str, force_reload: bool = False) -> dict:
    """Load electrode data from TXT file."""
    cache_key = f"electrode_txt_data_{dataset_name}"
    data = load_data(cache_key, force_reload=force_reload)

    if data is None:
        print(f"  Fetching electrode TXT data...")
        raw_data = get_electrode_data(node_id)
        data = parse_electrode_txt(raw_data)
        save_data(data, cache_key)

    return data


# =============================================================================
# Channel Row Building
# =============================================================================

def build_channel_row(
    pkg_name: str,
    sampling_freq: str,
    reference: str,
    ground: str
) -> dict:
    """
    Build a single channel row from package metadata.

    Uses ChannelsSidecar.ROW_DEFAULTS as base, then applies channel-specific values.
    """
    channel_name = clean_channel_name(pkg_name)
    channel_info = get_channel_info(channel_name)
    is_ekg = "ekg" in channel_name.lower()

    # Build row - ChannelsSidecar will apply defaults for missing fields
    row = {
        "name": channel_name,
        "type": channel_info["type"],
        "units": "uV",
        "low_cutoff": "n/a",
        "high_cutoff": "0.01" if not is_ekg else "n/a",
        "reference": "unknown" if is_ekg else reference,
        "ground": "unknown" if is_ekg else ground,
        "group": channel_info["group"],
        "sampling_frequency": sampling_freq,
        "notch": "n/a",
    }

    return row


# =============================================================================
# Output Path Building
# =============================================================================

def get_output_path(dataset_name: str, parent_key: str = None) -> Path:
    """
    Build the BIDS-compliant output path for channels.tsv.

    Args:
        dataset_name: Original dataset name (EPS or PennEPI format)
        parent_key: Sub-dataset key (e.g., "D01") for multi-day datasets

    Returns:
        Full path to the channels.tsv file
    """
    top_folder = eps_to_penn_epi(dataset_name)

    base_path = Path(OUTPUT_DIR) / top_folder / f"primary/sub-{top_folder}/ses-postimplant/ieeg"

    if parent_key:
        base_path = base_path / parent_key

    return base_path / f"sub-{top_folder}_ses-postimplant_task-clinical_channels.tsv"


# =============================================================================
# Main Processing Functions
# =============================================================================

def build_parent_id_ref(
    datasets: list,
    payload: dict,
    parent_id_reference: dict,
    force_reload: bool = False
) -> None:
    """
    Build parent ID reference mapping for multi-day datasets.

    This maps package IDs to sub-dataset keys (D01, D02, etc.) for datasets
    that have multiple recording days.
    """
    for ds in datasets:
        dataset_name = ds["content"]["name"]
        ds_id = ds["content"]["id"]

        payload[dataset_name] = {}
        parent_id_reference[dataset_name] = {}

        packages = load_data(f"package_{dataset_name}", force_reload=force_reload)
        if packages is None:
            packages = get_dataset_packages(ds_id)
            save_data(packages, f"package_{dataset_name}")

        for pkg in packages:
            pkg_content = pkg.get("content", {})
            pkg_name = pkg_content.get("name", "")

            if pkg_name.lower().strip().startswith("d0"):
                # Multi-day dataset: map package ID to day key
                parent_id_reference[dataset_name][pkg_content.get("id", "")] = pkg_name
                payload[dataset_name][pkg_name] = {
                    "sampling_frequency": None,
                    "duration": None,
                }
            else:
                # Single dataset
                payload[dataset_name].update({
                    "sampling_frequency": None,
                    "duration": None,
                })


def process_dataset(
    ds: dict,
    master_map: dict,
    payload: dict,
    parent_id_reference: dict,
    force_reload: bool = False
) -> None:
    """
    Process a single dataset to generate channels.tsv files.

    This function:
    1. Loads packages and classifies them by type
    2. Extracts metadata from ieeg.json and electrode files
    3. Builds channel rows from .mef files
    4. Writes channels.tsv using ChannelsSidecar
    """
    dataset_name = ds["content"]["name"]
    ds_id = ds["content"]["id"]

    print(f"Processing: {dataset_name}")

    # Skip invalid datasets
    if dataset_name in SKIP_DATASETS:
        print(f"  Skipping (in skip list)")
        return

    if not is_valid_dataset_name(dataset_name):
        print(f"  Skipping (not EPS/PennEPI)")
        return

    # Load packages
    packages = load_data(f"package_{dataset_name}", force_reload=force_reload)
    if packages is None:
        print(f"  Fetching packages from network...")
        packages = get_dataset_packages(ds_id)
        save_data(packages, f"package_{dataset_name}")

    # Get reference/ground from master map
    master_map_key = penn_epi_to_eps(dataset_name)
    reference, ground = master_map.get(master_map_key, ("unknown", "unknown"))

    # Track sampling frequency per parent (for multi-day datasets)
    sampling_freq_by_parent = {}
    default_sampling_freq = "n/a"

    # Collect channel rows by parent ID
    rows_by_parent = {}

    # Single pass through packages
    for pkg in packages:
        pkg_content = pkg.get("content", {})

        if is_deleted(pkg_content):
            continue

        pkg_name = pkg_content.get("name", "")
        pkg_type = classify_package(pkg_name)

        if pkg_type is None:
            continue

        node_id = pkg_content.get("nodeId")
        parent_id = pkg_content.get("parentId")

        # Handle ieeg.json - extract sampling frequency
        if pkg_type == "ieeg_json":
            metadata = load_ieeg_metadata(node_id, force_reload)
            sampling_freq = metadata.get("sampling_frequency", "n/a")
            duration = metadata.get("duration", "n/a")

            # Store in appropriate location
            parent_key = parent_id_reference[dataset_name].get(parent_id)
            if parent_key in MULTI_DAY_KEYS:
                sampling_freq_by_parent[parent_id] = sampling_freq
                payload[dataset_name][parent_key]["sampling_frequency"] = sampling_freq
                payload[dataset_name][parent_key]["duration"] = duration
            else:
                default_sampling_freq = sampling_freq
                payload[dataset_name]["sampling_frequency"] = sampling_freq
                payload[dataset_name]["duration"] = duration

        # Handle electrode files - just cache them for later use
        elif pkg_type == "electrodes_csv":
            load_electrode_csv(node_id, dataset_name, force_reload)

        elif pkg_type == "electrodes_txt":
            load_electrode_txt(node_id, dataset_name, force_reload)

        # Handle .mef files - build channel rows
        elif pkg_type == "mef":
            # Get sampling frequency for this parent
            freq = sampling_freq_by_parent.get(parent_id, default_sampling_freq)

            row = build_channel_row(pkg_name, freq, reference, ground)
            rows_by_parent.setdefault(parent_id, []).append(row)

    # Write channels.tsv files
    is_multi_day = any(k in payload[dataset_name] for k in MULTI_DAY_KEYS)

    for parent_id, rows in rows_by_parent.items():
        # Sort rows by channel name
        rows.sort(key=lambda r: r["name"].lower())

        # Determine output path
        if is_multi_day:
            parent_key = parent_id_reference[dataset_name].get(parent_id)
            if not parent_key:
                continue
            payload[dataset_name][parent_key]["row_data"] = rows
            output_path = get_output_path(dataset_name, parent_key)
        else:
            payload[dataset_name]["row_data"] = rows
            output_path = get_output_path(dataset_name)

        # Use ChannelsSidecar to write the file
        sidecar = ChannelsSidecar(rows=rows)
        sidecar.save(output_path=str(output_path))
        print(f"  Wrote {len(rows)} channels → {output_path.name}")


def make_channels(force_reload: bool = False) -> None:
    """
    Main entry point: generate channels.tsv for all datasets.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Fetching all datasets...")
    datasets = load_data("datasets", force_reload=force_reload)
    if datasets is None:
        print("Fetching datasets from network...")
        datasets = get_all_datasets()
        save_data(datasets, "datasets")

    print(f"Total datasets: {len(datasets)}")

    # Load reference/ground mapping
    master_map = get_ref_gnd_map(MASTER_CSV_PATH)

    # Initialize tracking structures
    payload = {}
    parent_id_reference = {}

    # Build parent ID reference for multi-day datasets
    build_parent_id_ref(datasets, payload, parent_id_reference, force_reload)

    # Process each dataset
    for ds in datasets:
        process_dataset(ds, master_map, payload, parent_id_reference, force_reload)

    # Save final payload
    save_data(payload, "payload")
    print("\n✅ Done\n")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate channels TSV files')
    parser.add_argument(
        '--force-reload',
        action='store_true',
        help='Force reload all data from network, bypassing cache'
    )
    args = parser.parse_args()

    if args.force_reload:
        print("Force reload enabled - all data will be fetched from network")

    make_channels(force_reload=args.force_reload)
