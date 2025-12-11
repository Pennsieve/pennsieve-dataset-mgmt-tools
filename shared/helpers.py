"""
Helper functions for Pennsieve dataset management.

All API functions require authentication via auth.authenticate() before use.
"""

import csv
import io
import json
import os
import re
import string
import requests

from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import quote

from .config import API_HOST, PAGE_SIZE, CACHE_DIR, OUTPUT_DIR
from .auth import get_headers, get_auth


# =============================================================================
# API Functions
# =============================================================================

def get_all_datasets(headers: Optional[Dict[str, str]] = None) -> list:
    """
    Paginate through all datasets from Pennsieve API.

    Args:
        headers: Optional custom headers dict. If None, uses global auth via get_headers().

    Requires: Either pass headers or call auth.authenticate() first.
    """
    if headers is None:
        headers = get_headers()

    datasets = []
    offset = 0

    while True:
        url = (
            f"{API_HOST}/datasets/paginated"
            f"?limit={PAGE_SIZE}&offset={offset}&orderBy=Name&orderDirection=Asc"
            f"&includeBannerUrl=false&includePublishedDataset=false"
        )
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        batch = data.get("datasets", [])
        if not batch:
            break

        datasets.extend(batch)
        offset += PAGE_SIZE
        if offset >= data.get("totalCount", 0):
            break

    return datasets


def find_dataset_by_name(name: str, all_datasets: list) -> Optional[Dict]:
    """
    Find a dataset by its name.

    Args:
        name: Dataset name to search for
        all_datasets: List of datasets from get_all_datasets()

    Returns:
        Dataset object if found, None otherwise
    """
    for ds in all_datasets:
        content = ds.get("content", {})
        if content.get("name", "").strip() == name:
            return ds
    return None


def get_dataset_packages(dataset_id: str) -> list:
    """
    Return all packages for a dataset, handling pagination.

    Requires: auth.authenticate() must be called first.
    """
    encoded_id = quote(dataset_id, safe="")
    base_url = (
        f"{API_HOST}/datasets/{encoded_id}/packages?"
        f"pageSize=1000&includeSourceFiles=false"
    )

    all_packages = []
    cursor = None

    while True:
        url = f"{base_url}&cursor={cursor}" if cursor else base_url
        response = requests.get(url, headers=get_headers())
        response.raise_for_status()

        data = response.json()
        all_packages.extend(data.get('packages', []))

        cursor = data.get('cursor')
        if not cursor:
            break

    return all_packages


def get_freq_duration(node_id: str) -> dict:
    """
    Get sampling frequency and duration from an iEEG JSON file.

    Requires: auth.authenticate() must be called first.
    """
    url = f"{API_HOST}/packages/download-manifest"

    payload = {"nodeIds": [node_id]}

    response = requests.post(url, json=payload, headers=get_headers())
    response_json = response.json()
    data = response_json["data"]

    download_url = None
    for item in data:
        download_url = item["url"]

    response = requests.get(download_url)
    response.raise_for_status()
    ieeg_json = response.json()

    sampling_frequency = ieeg_json.get("SamplingFrequency", "n/a")
    duration = ieeg_json.get("RecordingDuration", "n/a")

    return {"sampling_frequency": sampling_frequency, "duration": duration}


def get_electrode_data(node_id: str) -> list:
    """
    Download and parse electrode CSV data from a package.

    Requires: auth.authenticate() must be called first.
    """
    manifest_url = f"{API_HOST}/packages/download-manifest"

    payload = {"nodeIds": [node_id]}

    resp = requests.post(manifest_url, json=payload, headers=get_headers())
    resp.raise_for_status()

    manifest = resp.json()
    data = manifest.get("data", [])
    if not data:
        raise ValueError("No download URLs returned from Pennsieve API.")

    download_url = data[0].get("url")
    if not download_url:
        raise ValueError("Manifest response missing 'url' key.")

    file_resp = requests.get(download_url)
    file_resp.raise_for_status()

    csv_text = file_resp.text
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)

    return rows


# =============================================================================
# Caching Functions
# =============================================================================

def save_data(data: Any, name: str) -> None:
    """Save data to a JSON file in the cache directory."""
    file_path = os.path.join(CACHE_DIR, f"{name}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_data(name: str, force_reload: bool = False) -> Optional[Any]:
    """
    Load cached data if it exists.

    Args:
        name: Cache file name (without .json extension)
        force_reload: If True, bypass cache and return None

    Returns:
        Cached data or None if not found/force_reload=True
    """
    if force_reload:
        return None

    file_path = os.path.join(CACHE_DIR, f"{name}.json")
    if not os.path.exists(file_path):
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load cache ({e})")
        return None


# =============================================================================
# Name Conversion Functions
# =============================================================================

def eps_to_penn_epi(dataset_name: str) -> str:
    """Convert dataset name like 'EPS00049' -> 'PennEPI00049'."""
    match = re.search(r"(\d+)", dataset_name)
    num = match.group(1) if match else "00000"
    return f"PennEPI{int(num):05d}"


def penn_epi_to_eps(dataset_name: str) -> str:
    """Convert dataset name like 'PennEPI00049' -> 'EPS0000049'."""
    match = re.search(r"(\d+)", dataset_name)
    num = match.group(1) if match else "0000000"
    return f"EPS{int(num):07d}"


def generate_new_name(old_name: str) -> str:
    """
    Generate a new PennEPI dataset name from an old EPS-style name.

    Examples:
        EPS0000215  -> PennEPI00215
        EPS00049    -> PennEPI00049
    """
    old_name = old_name.strip()

    match = re.match(r"(?i)^EPS[_\s]*(\d+)$", old_name)
    if not match:
        return old_name

    numeric_part = match.group(1).lstrip("0") or "0"
    new_suffix = numeric_part.zfill(5)

    return f"PennEPI{new_suffix}"


# =============================================================================
# String Utilities
# =============================================================================

def sanitize_group_name(name: str) -> str:
    """Strip punctuation and spaces from name for group column."""
    return re.sub(r"[^\w]", "", name)


def clean_channel_name(pkg_name: str) -> str:
    """Clean a channel/package name for comparison."""
    name = pkg_name.lower().removesuffix(".mef")
    name = re.sub(r"(eeg|-ref)", "", name)
    name = name.translate(str.maketrans("", "", string.punctuation))
    return name.strip().upper()


# =============================================================================
# Channel Configuration
# =============================================================================

CHANNEL_CONFIGS = {
    "ECG": {
        "names": ["EKG", "EKG1", "EKG2", "ECG", "ECG1", "ECG2"],
        "type": "ECG",
        "group": "n/a",
    },
    "EEG": {
        "names": ["C3", "C03", "C4", "C04", "CZ", "F3", "F4", "F7", "F8",
                  "FP1", "FP2", "FZ", "O1", "O2", "P3", "P4", "PZ",
                  "T3", "T4", "T5", "T6"],
        "type": "EEG",
        "group_strategy": "use_name",
    },
    "EOG": {
        "names": ["LOC", "ROC"],
        "type": "EOG",
        "group_strategy": "use_name",
    },
    "EMG": {
        "names": ["EMG", "EMG1", "EMG2"],
        "type": "EMG",
        "group_strategy": "use_name",
    },
}

# Build reverse lookup once at module load
CHANNEL_LOOKUP = {}
for config_name, config in CHANNEL_CONFIGS.items():
    for channel_name in config["names"]:
        CHANNEL_LOOKUP[channel_name] = {
            "type": config["type"],
            "group": channel_name if config.get("group_strategy") == "use_name"
                     else config.get("group", "n/a")
        }


def get_channel_info(channel_name: str) -> dict:
    """Get type and group for a channel name."""
    if channel_name.upper() in ["REF", "GND"]:
        return {"type": "unknown", "group": "unknown"}

    if channel_name in CHANNEL_LOOKUP:
        return CHANNEL_LOOKUP[channel_name]

    # Default to SEEG with group = first 2 letters
    group = channel_name[:2] if len(channel_name) >= 2 else channel_name
    return {"type": "SEEG", "group": group}


# =============================================================================
# CSV Parsing Functions
# =============================================================================

def multi_dataset_read_csv_to_dict(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Read master CSV and group rows by EPS Number and sub-dataset.

    If an EPS has only one sub-dataset, it's flattened into the root.
    """
    data: Dict[str, Dict[str, Dict[str, Any]]] = {}

    with path.open(newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            eps = (row.get("EPS Number") or "").strip()
            subdataset = (row.get("Dataset") or "").strip() or "XX"

            if not eps:
                continue

            if eps not in data:
                data[eps] = {}

            data[eps][subdataset] = {
                k: v for k, v in row.items() if k not in ("EPS Number", "Dataset")
            }

    # Flatten EPS entries with only one subdataset
    flattened: Dict[str, Dict[str, Any]] = {}
    for eps, subdatasets in data.items():
        if len(subdatasets) == 1:
            only_key = next(iter(subdatasets))
            flattened[eps] = subdatasets[only_key]
        else:
            flattened[eps] = subdatasets

    return flattened


def read_csv_to_dict(path: Path) -> Dict[str, Dict[str, Any]]:
    """Read CSV keyed by participant_id."""
    data = {}
    with path.open(newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            eps = row.get("participant_id")
            if not eps:
                continue
            data[eps.strip()] = {k: v for k, v in row.items() if k != "participant_id"}
    return data


def parse_electrode_txt(data) -> Dict[str, Dict[str, Any]]:
    """Parse electrode text data into a dictionary keyed by label."""

    def parse_line(line):
        parts = line.split("\t")
        if len(parts) < 6:
            return None
        return {
            "label": parts[0],
            "x": float(parts[1]),
            "y": float(parts[2]),
            "z": float(parts[3]),
            "type": parts[4],
            "group": parts[5]
        }

    parsed = []

    if isinstance(data, str):
        lines = data.strip().splitlines()
        for line in lines:
            item = parse_line(line)
            if item:
                parsed.append(item)

    elif isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                for k, v in entry.items():
                    for line in (k, v):
                        item = parse_line(line)
                        if item:
                            parsed.append(item)
            elif isinstance(entry, str):
                item = parse_line(entry)
                if item:
                    parsed.append(item)

    result = {
        item["label"]: {k: v for k, v in item.items() if k != "label"}
        for item in parsed
    }
    return result
