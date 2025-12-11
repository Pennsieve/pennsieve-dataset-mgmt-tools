#!/usr/bin/env python3
"""
Metadata Manager

Unified tool for managing Pennsieve dataset metadata models:
- Create models from templates
- Populate models with data from files
- Delete models (and their records)
- List models in datasets

Usage:
  # List models in a dataset
  python metadata_manager.py --api-key KEY --api-secret SECRET \
      list --datasets PennEPI00949

  # Delete all models from datasets
  python metadata_manager.py --api-key KEY --api-secret SECRET \
      delete --datasets PennEPI00949 --execute

  # Delete specific models only
  python metadata_manager.py --api-key KEY --api-secret SECRET \
      delete --datasets PennEPI00949 --models person eeg --execute

  # Create and populate a model from template
  python metadata_manager.py --api-key KEY --api-secret SECRET \
      populate --datasets PennEPI00086 \
      --file-pattern _ieeg.json \
      --template-id TEMPLATE_UUID \
      --model-name bids_ieeg_sidecar --display-name "BIDS iEEG sidecar"

  # Populate using a config file
  python metadata_manager.py --api-key KEY --api-secret SECRET \
      populate --datasets PennEPI00014 --config models_config.json
"""

import argparse
import csv
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

# Set up import paths - local first, then parent for shared package
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir))
sys.path.insert(1, str(_this_dir.parent))

from shared.config import API_HOST, PAGE_SIZE
from shared.auth import PennsieveAuth
from shared.helpers import load_data, save_data, get_all_datasets, find_dataset_by_name

API2_BASE_URL = "https://api2.pennsieve.io"


class MetadataManager:
    """Manages metadata models in Pennsieve datasets."""

    def __init__(
        self,
        auth: PennsieveAuth,
        dry_run: bool = True,
        force_reload: bool = False,
        verbose: bool = False
    ):
        self.auth = auth
        self.dry_run = dry_run
        self.force_reload = force_reload
        self.verbose = verbose

    def _log(self, message: str, indent: int = 0):
        """Print log message with optional indentation."""
        prefix = "  " * indent
        print(f"{prefix}{message}")

    def _debug(self, message: str, indent: int = 0):
        """Print debug message if verbose mode is on."""
        if self.verbose:
            self._log(f"[DEBUG] {message}", indent)

    # -------------------------------------------------------------------------
    # Dataset Operations
    # -------------------------------------------------------------------------

    def get_all_datasets_cached(self) -> List[Dict]:
        """Get all datasets, with caching."""
        datasets = load_data("datasets", force_reload=self.force_reload)
        if datasets is None:
            self._log("Fetching datasets from network...")
            datasets = get_all_datasets(headers=self.auth.get_headers())
            save_data(datasets, "datasets")
        return datasets

    def find_dataset(self, name: str, all_datasets: List[Dict]) -> Optional[Dict]:
        """Find a dataset by name using shared helper."""
        return find_dataset_by_name(name, all_datasets)

    def get_dataset_packages(self, dataset_id: str) -> List[Dict]:
        """Return all packages for a dataset, handling pagination."""
        encoded_id = quote(dataset_id, safe="")
        base_url = f"{API_HOST}/datasets/{encoded_id}/packages?pageSize=1000&includeSourceFiles=false"

        all_packages = []
        cursor = None

        while True:
            url = f"{base_url}&cursor={cursor}" if cursor else base_url
            response = requests.get(url, headers=self.auth.get_headers())
            response.raise_for_status()

            data = response.json()
            all_packages.extend(data.get('packages', []))

            cursor = data.get('cursor')
            if not cursor:
                break

        return all_packages

    # -------------------------------------------------------------------------
    # Model Operations
    # -------------------------------------------------------------------------

    def get_models_for_dataset(self, dataset_id: str) -> List[Dict]:
        """Get all models for a dataset."""
        encoded_dataset_id = quote(dataset_id, safe="")
        url = f"{API2_BASE_URL}/metadata/models?dataset_id={encoded_dataset_id}"

        response = requests.get(url, headers=self.auth.get_headers())
        response.raise_for_status()

        return response.json()

    def get_existing_model_by_name(self, dataset_id: str, model_name: str) -> Optional[str]:
        """Find an existing model by name in a dataset."""
        models = self.get_models_for_dataset(dataset_id)
        for item in models:
            model = item.get("model", {})
            if model.get("name") == model_name:
                return model.get("id")
        return None

    def create_model_from_template(
        self,
        template_id: str,
        dataset_id: str,
        model_name: str,
        display_name: str,
        description: str = "",
        template_version: int = 1
    ) -> Optional[str]:
        """Create a model from a template."""
        encoded_dataset_id = quote(dataset_id, safe="")
        url = (
            f"{API2_BASE_URL}/metadata/templates/{template_id}/models"
            f"?dataset_id={encoded_dataset_id}&version={template_version}"
        )
        payload = {
            "name": model_name,
            "display_name": display_name,
            "description": description,
        }

        self._log(f"    URL: {url}")
        self._log(f"    Payload: {json.dumps(payload, indent=2)}")

        if self.dry_run:
            self._log(f"    [DRY-RUN] Would POST")
            return "dry-run-model-id"

        response = requests.post(url, json=payload, headers=self.auth.get_headers())

        # Check for duplicate model name error
        if response.status_code == 400:
            try:
                error_body = response.json()
                if "duplicate model name" in error_body.get("message", ""):
                    self._log(f"    Model already exists, finding existing model ID...")
                    existing_id = self.get_existing_model_by_name(dataset_id, model_name)
                    if existing_id:
                        self._log(f"    Found existing model (ID: {existing_id})")
                        return existing_id
            except json.JSONDecodeError:
                pass

        if not response.ok:
            self._log(f"    Response status: {response.status_code}")
            self._log(f"    Response body: {response.text}")
        response.raise_for_status()

        result = response.json()
        model_id = result.get("model", {}).get("id")

        if not model_id:
            raise ValueError(f"Could not extract model ID from response: {result}")

        self._log(f"    Model created successfully (ID: {model_id})")
        return model_id

    def delete_model(self, model_id: str, dataset_id: str, force: bool = True) -> bool:
        """Delete a model from a dataset."""
        encoded_dataset_id = quote(dataset_id, safe="")
        force_param = "true" if force else "false"
        url = f"{API2_BASE_URL}/metadata/models/{model_id}?dataset_id={encoded_dataset_id}&force={force_param}"

        self._debug(f"DELETE {url}", indent=3)

        if self.dry_run:
            return True

        response = requests.delete(url, headers=self.auth.get_headers())

        if not response.ok:
            self._log(f"ERROR deleting model: {response.status_code}", indent=3)
            self._log(f"Response: {response.text}", indent=3)
            return False

        return True

    def post_records(
        self,
        model_id: str,
        records: List[Dict],
        dataset_id: str
    ) -> bool:
        """POST records to a model."""
        encoded_dataset_id = quote(dataset_id, safe="")
        url = f"{API2_BASE_URL}/metadata/models/{model_id}/records?dataset_id={encoded_dataset_id}"
        payload = {"records": records}

        self._log(f"    URL: {url}")
        self._log(f"    Payload: {json.dumps(payload, indent=2)}")

        if self.dry_run:
            self._log(f"    [DRY-RUN] Would POST {len(records)} records")
            return True

        response = requests.post(url, json=payload, headers=self.auth.get_headers())
        if not response.ok:
            self._log(f"    Response status: {response.status_code}")
            self._log(f"    Response body: {response.text}")
        response.raise_for_status()
        self._log(f"    Posted {len(records)} records successfully")
        return True

    # -------------------------------------------------------------------------
    # File Operations
    # -------------------------------------------------------------------------

    def get_package_path(self, package: Dict, all_packages: List[Dict]) -> str:
        """Reconstruct the path to a package by walking up parent IDs."""
        pkg_lookup = {}
        for pkg in all_packages:
            content = pkg.get("content", {})
            pkg_id = content.get("id")
            if pkg_id:
                pkg_lookup[pkg_id] = pkg

        path_parts = []
        current = package

        while True:
            content = current.get("content", {})
            parent_id = content.get("parentId")

            if not parent_id or parent_id not in pkg_lookup:
                break

            parent = pkg_lookup[parent_id]
            parent_content = parent.get("content", {})
            parent_name = parent_content.get("name", "")

            if parent_name:
                path_parts.insert(0, parent_name)

            current = parent

        return "/".join(path_parts)

    def find_target_file(
        self,
        packages: List[Dict],
        filename_pattern: str
    ) -> Optional[Dict]:
        """Find the target file package by matching a filename pattern."""
        for pkg in packages:
            content = pkg.get("content", {})
            name = content.get("name", "")

            if name.startswith("__DELETED__"):
                continue

            pkg_path = self.get_package_path(pkg, packages)
            if "archive" in pkg_path.lower():
                continue

            if name == filename_pattern or name.endswith(filename_pattern):
                return pkg

        return None

    def download_file_content(self, node_id: str) -> str:
        """Download file content from Pennsieve."""
        manifest_url = f"{API_HOST}/packages/download-manifest"
        payload = {"nodeIds": [node_id]}

        response = requests.post(
            manifest_url,
            json=payload,
            headers=self.auth.get_headers()
        )
        response.raise_for_status()

        manifest = response.json()
        data = manifest.get("data", [])
        if not data:
            raise ValueError(f"No download URLs returned for node {node_id}")

        download_url = data[0].get("url")
        if not download_url:
            raise ValueError("Manifest response missing 'url' key")

        file_response = requests.get(download_url)
        file_response.raise_for_status()

        return file_response.text

    # -------------------------------------------------------------------------
    # Data Transformation
    # -------------------------------------------------------------------------

    def csv_to_json(self, csv_content: str) -> List[Dict[str, Any]]:
        """Convert CSV/TSV content to a list of JSON records."""
        dialect = csv.Sniffer().sniff(csv_content[:1024], delimiters=',\t')
        reader = csv.DictReader(io.StringIO(csv_content), dialect=dialect)
        return list(reader)

    def transform_record(
        self,
        record: Dict[str, Any],
        filename: Optional[str] = None,
        is_ieeg_sidecar: bool = False
    ) -> Dict[str, Any]:
        """Apply transformations to a record."""
        if is_ieeg_sidecar:
            if filename:
                record_id = filename.rsplit('.', 1)[0] if '.' in filename else filename
                record["id"] = record_id

            if "SamplingFrequency" in record:
                record["SamplingFrequency"] = str(record["SamplingFrequency"])

            if "HardwareFilters" in record and isinstance(record["HardwareFilters"], dict):
                for filter_name, filter_values in record["HardwareFilters"].items():
                    if isinstance(filter_values, dict):
                        for key in ["min (Hz)", "max (Hz)"]:
                            if key in filter_values:
                                filter_values[key] = str(filter_values[key])

            return record

        # Non-ieeg sidecar transformations
        if "age_intervention" in record:
            try:
                record["age_intervention"] = float(record["age_intervention"])
            except (ValueError, TypeError):
                del record["age_intervention"]

        if record.get("species") == "home sapiens":
            record["species"] = "homo sapiens"

        for field in ["seizure_Engel12m", "seizure_Engel24m"]:
            if field in record:
                val = record[field]
                if val != "n/a":
                    try:
                        record[field] = float(val)
                    except (ValueError, TypeError):
                        del record[field]

        if "fiveSenseScore" in record:
            val = record["fiveSenseScore"]
            if val != "n/a":
                try:
                    record["fiveSenseScore"] = float(val)
                except (ValueError, TypeError):
                    del record["fiveSenseScore"]

        if "SamplingFrequency" in record:
            try:
                record["SamplingFrequency"] = float(record["SamplingFrequency"])
            except (ValueError, TypeError):
                pass

        if "HardwareFilters" in record and isinstance(record["HardwareFilters"], dict):
            for filter_name, filter_values in record["HardwareFilters"].items():
                if isinstance(filter_values, dict):
                    for key in ["min (Hz)", "max (Hz)"]:
                        if key in filter_values:
                            try:
                                filter_values[key] = float(filter_values[key])
                            except (ValueError, TypeError):
                                pass

        required_fields = {"participant_id", "species", "population", "sex"}
        keys_to_remove = [
            key for key, value in record.items()
            if value == "" and key not in required_fields
        ]
        for key in keys_to_remove:
            del record[key]

        participants_fields = {
            "participant_id", "species", "population", "sex",
            "MRI_lesion", "MRI_lesionType", "MRI_lesionDetails",
            "ieeg_isFocal", "age_intervention", "intervention_type",
            "intervention_side", "intervention_location",
            "seizure_Engel12m", "seizure_Engel24m", "fiveSenseScore"
        }

        if "participant_id" in record:
            unknown_keys = [k for k in record.keys() if k not in participants_fields]
            for key in unknown_keys:
                del record[key]

        if "Authors" in record and isinstance(record["Authors"], list):
            transformed_authors = []
            for author in record["Authors"]:
                if isinstance(author, str):
                    parts = author.strip().split(None, 1)
                    if len(parts) == 2:
                        transformed_authors.append({
                            "first_name": parts[0],
                            "last_name": parts[1]
                        })
                    elif len(parts) == 1 and parts[0]:
                        transformed_authors.append({
                            "first_name": "",
                            "last_name": parts[0]
                        })
                elif isinstance(author, dict):
                    transformed_authors.append(author)
            record["Authors"] = transformed_authors if transformed_authors else []

        return record

    def extract_data(
        self,
        file_content: str,
        filename: str,
        is_ieeg_sidecar: bool = False
    ) -> List[Dict[str, Any]]:
        """Extract and transform data from file content."""
        if filename.endswith('.json'):
            data = json.loads(file_content)
            records = [data] if isinstance(data, dict) else data
        elif filename.endswith('.csv') or filename.endswith('.tsv'):
            records = self.csv_to_json(file_content)
        else:
            try:
                records = self.csv_to_json(file_content)
            except Exception:
                records = json.loads(file_content)

        return [
            self.transform_record(r, filename=filename, is_ieeg_sidecar=is_ieeg_sidecar)
            for r in records
        ]

    # -------------------------------------------------------------------------
    # High-Level Commands
    # -------------------------------------------------------------------------

    def list_models(
        self,
        dataset_names: Optional[List[str]] = None,
        dataset_prefix: Optional[str] = None
    ) -> None:
        """List models in datasets."""
        all_datasets = self.get_all_datasets_cached()
        datasets_to_process = self._filter_datasets(all_datasets, dataset_names, dataset_prefix)

        for ds_name in datasets_to_process:
            dataset = self.find_dataset(ds_name, all_datasets)
            if not dataset:
                continue

            dataset_id = dataset.get("content", {}).get("id")
            if not dataset_id:
                continue

            self._log(f"\n{ds_name}:")
            models = self.get_models_for_dataset(dataset_id)

            if not models:
                self._log("  (no models)", indent=1)
                continue

            for item in models:
                model = item.get("model", {})
                model_name = model.get("name", "unknown")
                model_id = model.get("id", "unknown")
                record_count = item.get("recordCount", 0)
                self._log(f"  - {model_name} (records: {record_count}, id: {model_id})")

    def delete_models(
        self,
        dataset_names: Optional[List[str]] = None,
        dataset_prefix: Optional[str] = None,
        model_filter: Optional[List[str]] = None
    ) -> Tuple[int, int, int]:
        """Delete models from datasets."""
        self._log("="*60)
        self._log("MODEL DELETER")
        self._log("="*60)
        self._log(f"Model filter: {model_filter or 'ALL MODELS'}")
        self._log(f"Dry run: {self.dry_run}")
        self._log("="*60)

        if not self.dry_run:
            self._log("")
            self._log("WARNING: This will permanently delete models and their records!")
            self._log("")

        all_datasets = self.get_all_datasets_cached()
        datasets_to_process = self._filter_datasets(all_datasets, dataset_names, dataset_prefix)

        if not datasets_to_process:
            self._log("No datasets matched the criteria")
            return (0, 0, 0)

        self._log(f"Datasets to process: {len(datasets_to_process)}")

        total_success = 0
        total_failures = 0

        for ds_name in datasets_to_process:
            success, failures = self._delete_models_from_dataset(
                ds_name, all_datasets, model_filter
            )
            total_success += success
            total_failures += failures

        self._log(f"\n{'='*60}")
        self._log("SUMMARY")
        self._log(f"{'='*60}")
        self._log(f"Datasets processed: {len(datasets_to_process)}")
        self._log(f"Models deleted: {total_success}")
        self._log(f"Failures: {total_failures}")

        if self.dry_run:
            self._log("\n[DRY-RUN MODE] No actual changes were made")

        return (len(datasets_to_process), total_success, total_failures)

    def populate_models(
        self,
        dataset_names: Optional[List[str]] = None,
        dataset_prefix: Optional[str] = None,
        model_configs: Optional[List[Dict]] = None,
        is_ieeg_sidecar: bool = False
    ) -> Tuple[int, int, int]:
        """Create and populate models from templates."""
        if self.dry_run:
            self._log("\n" + "="*60)
            self._log("DRY RUN MODE - No actual changes will be made")
            self._log("="*60)

        all_datasets = self.get_all_datasets_cached()
        datasets_to_process = self._filter_datasets(all_datasets, dataset_names, dataset_prefix)

        if not datasets_to_process:
            self._log("No datasets matched the criteria")
            return (0, 0, 0)

        self._log(f"Datasets to process: {len(datasets_to_process)}")
        self._log(f"Models to process: {len(model_configs)}")

        success_count = 0
        fail_count = 0

        for ds_name in datasets_to_process:
            for model_cfg in model_configs:
                try:
                    success = self._populate_model_in_dataset(
                        ds_name, all_datasets, model_cfg, is_ieeg_sidecar
                    )
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    self._log(f"  ERROR: {e}")
                    fail_count += 1

        self._log(f"\n{'='*60}")
        self._log("SUMMARY")
        self._log(f"{'='*60}")
        self._log(f"Successful: {success_count}")
        self._log(f"Failed: {fail_count}")

        return (len(datasets_to_process), success_count, fail_count)

    # -------------------------------------------------------------------------
    # Private Helpers
    # -------------------------------------------------------------------------

    def _filter_datasets(
        self,
        all_datasets: List[Dict],
        dataset_names: Optional[List[str]],
        dataset_prefix: Optional[str]
    ) -> List[str]:
        """Filter datasets by name or prefix."""
        datasets_to_process = []
        for ds in all_datasets:
            ds_name = ds.get("content", {}).get("name", "")
            if dataset_names and ds_name in dataset_names:
                datasets_to_process.append(ds_name)
            elif dataset_prefix and ds_name.startswith(dataset_prefix):
                datasets_to_process.append(ds_name)
        return datasets_to_process

    def _delete_models_from_dataset(
        self,
        dataset_name: str,
        all_datasets: List[Dict],
        model_filter: Optional[List[str]] = None
    ) -> Tuple[int, int]:
        """Delete models from a single dataset."""
        self._log(f"\n{'='*60}")
        self._log(f"Processing dataset: {dataset_name}")
        self._log(f"{'='*60}")

        dataset = self.find_dataset(dataset_name, all_datasets)
        if not dataset:
            self._log(f"ERROR: Dataset not found: {dataset_name}", indent=1)
            return (0, 0)

        dataset_id = dataset.get("content", {}).get("id")
        if not dataset_id:
            self._log(f"ERROR: Could not get dataset ID", indent=1)
            return (0, 0)

        self._log(f"Dataset ID: {dataset_id}", indent=1)

        models = self.get_models_for_dataset(dataset_id)
        self._log(f"Found {len(models)} models", indent=1)

        if not models:
            self._log("No models to delete", indent=1)
            return (0, 0)

        success_count = 0
        failure_count = 0

        for item in models:
            model = item.get("model", {})
            model_id = model.get("id")
            model_name = model.get("name", "unknown")

            if model_filter and model_name not in model_filter:
                self._debug(f"Skipping model '{model_name}' (not in filter)", indent=2)
                continue

            self._log(f"Deleting model: {model_name} (ID: {model_id})", indent=2)

            if self.dry_run:
                self._log(f"[DRY-RUN] Would delete model: {model_name}", indent=3)
                success_count += 1
            else:
                if self.delete_model(model_id, dataset_id, force=True):
                    self._log(f"Deleted successfully", indent=3)
                    success_count += 1
                else:
                    failure_count += 1

        return (success_count, failure_count)

    def _populate_model_in_dataset(
        self,
        dataset_name: str,
        all_datasets: List[Dict],
        model_cfg: Dict,
        is_ieeg_sidecar: bool = False
    ) -> bool:
        """Populate a model in a single dataset."""
        self._log(f"\n{'='*60}")
        self._log(f"Processing dataset: {dataset_name}")
        self._log(f"{'='*60}")

        dataset = self.find_dataset(dataset_name, all_datasets)
        if not dataset:
            self._log(f"  ERROR: Dataset not found: {dataset_name}")
            return False

        dataset_id = dataset.get("content", {}).get("id")
        if not dataset_id:
            self._log(f"  ERROR: Could not find dataset ID")
            return False

        self._log(f"  Dataset ID: {dataset_id}")

        # Step 1: Create or find model
        existing_model_id = model_cfg.get("model_id")
        if existing_model_id:
            self._log(f"\n  Step 1: Using existing model...")
            self._log(f"    Model ID: {existing_model_id}")
            model_id = existing_model_id
        else:
            self._log(f"\n  Step 1: Creating model from template...")
            template_id = model_cfg.get("template_id")
            model_name = model_cfg.get("model_name")
            display_name = model_cfg.get("display_name")

            try:
                model_id = self.create_model_from_template(
                    template_id,
                    dataset_id,
                    model_name=model_name,
                    display_name=display_name,
                    description=model_cfg.get("description", ""),
                    template_version=model_cfg.get("template_version", 1)
                )
            except requests.HTTPError as e:
                self._log(f"  ERROR: Failed to create model: {e}")
                return False

        # Step 2: Find target file
        file_pattern = model_cfg.get("file_pattern")
        self._log(f"\n  Step 2: Finding target file (pattern: {file_pattern})...")

        packages = load_data(f"packages_{dataset_name}", force_reload=self.force_reload)
        if packages is None:
            self._log(f"    Fetching packages from network...")
            packages = self.get_dataset_packages(dataset_id)
            save_data(packages, f"packages_{dataset_name}")

        target_pkg = self.find_target_file(packages, file_pattern)
        if not target_pkg:
            self._log(f"  ERROR: No file matching pattern '{file_pattern}' found")
            return False

        target_name = target_pkg.get("content", {}).get("name")
        target_node_id = target_pkg.get("content", {}).get("nodeId")
        self._log(f"    Found: {target_name} (nodeId: {target_node_id})")

        # Step 3: Download and extract data
        self._log(f"\n  Step 3: Downloading and extracting data...")
        if not self.dry_run:
            try:
                file_content = self.download_file_content(target_node_id)
                records = self.extract_data(file_content, target_name, is_ieeg_sidecar)
                self._log(f"    Extracted {len(records)} records")
            except Exception as e:
                self._log(f"  ERROR: Failed to download/extract data: {e}")
                return False
        else:
            self._log(f"    [DRY-RUN] Would download and extract data from {target_name}")
            records = []

        # Step 4: Post records
        self._log(f"\n  Step 4: Posting records to model...")
        if not self.dry_run:
            try:
                self.post_records(model_id, records, dataset_id)
                self._log(f"  SUCCESS: Completed processing for {dataset_name}")
            except requests.HTTPError as e:
                self._log(f"  ERROR: Failed to post records: {e}")
                return False
        else:
            self._log(f"    [DRY-RUN] Would post records to model {model_id}")

        return True


def main():
    parser = argparse.ArgumentParser(
        description='Manage metadata models in Pennsieve datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Authentication
    parser.add_argument('--api-key', required=True, help='Pennsieve API key')
    parser.add_argument('--api-secret', required=True, help='Pennsieve API secret')

    # Common options
    parser.add_argument('--force-reload', action='store_true', help='Bypass cache')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # List command
    list_parser = subparsers.add_parser('list', help='List models in datasets')
    list_group = list_parser.add_mutually_exclusive_group(required=True)
    list_group.add_argument('--datasets', nargs='+', help='Dataset names')
    list_group.add_argument('--prefix', help='Dataset name prefix')

    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete models from datasets')
    delete_group = delete_parser.add_mutually_exclusive_group(required=True)
    delete_group.add_argument('--datasets', nargs='+', help='Dataset names')
    delete_group.add_argument('--prefix', help='Dataset name prefix')
    delete_parser.add_argument('--models', nargs='+', help='Specific models to delete')
    delete_mode = delete_parser.add_mutually_exclusive_group(required=True)
    delete_mode.add_argument('--dry-run', action='store_true', help='Preview changes')
    delete_mode.add_argument('--execute', action='store_true', help='Actually delete')

    # Populate command
    pop_parser = subparsers.add_parser('populate', help='Create and populate models')
    pop_group = pop_parser.add_mutually_exclusive_group(required=True)
    pop_group.add_argument('--datasets', nargs='+', help='Dataset names')
    pop_group.add_argument('--prefix', help='Dataset name prefix')
    pop_group.add_argument('--all', action='store_true', help='Process all datasets')
    pop_parser.add_argument('--file-pattern', help='File pattern to search for')
    pop_parser.add_argument('--template-id', help='Template ID')
    pop_parser.add_argument('--template-version', type=int, default=1, help='Template version')
    pop_parser.add_argument('--model-name', help='Model name')
    pop_parser.add_argument('--display-name', help='Display name')
    pop_parser.add_argument('--description', default='', help='Model description')
    pop_parser.add_argument('--model-id', help='Use existing model ID')
    pop_parser.add_argument('--config', help='Path to JSON config file')
    pop_parser.add_argument('--ieeg-sidecar', action='store_true', help='Enable ieeg sidecar mode')
    pop_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Authenticate
    print("Authenticating...")
    auth = PennsieveAuth()
    auth.authenticate(args.api_key, args.api_secret)
    print("Authentication successful")

    # Determine dry_run based on command
    if args.command == 'list':
        dry_run = True
    elif args.command == 'delete':
        dry_run = args.dry_run
    else:
        dry_run = getattr(args, 'dry_run', False)

    manager = MetadataManager(
        auth=auth,
        dry_run=dry_run,
        force_reload=args.force_reload,
        verbose=args.verbose
    )

    if args.command == 'list':
        manager.list_models(
            dataset_names=args.datasets,
            dataset_prefix=args.prefix
        )

    elif args.command == 'delete':
        datasets_processed, success, failures = manager.delete_models(
            dataset_names=args.datasets,
            dataset_prefix=args.prefix,
            model_filter=args.models
        )
        if failures > 0:
            sys.exit(1)
        if datasets_processed == 0:
            sys.exit(2)

    elif args.command == 'populate':
        # Build model configs
        if args.config:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
            model_configs = config_data.get("models", [])
        else:
            if not args.file_pattern:
                parser.error("--file-pattern is required unless --config is provided")
            if not args.model_id and not args.template_id:
                parser.error("--template-id is required unless --model-id or --config is provided")

            model_configs = [{
                "template_id": args.template_id,
                "template_version": args.template_version,
                "model_name": args.model_name,
                "display_name": args.display_name,
                "description": args.description,
                "file_pattern": args.file_pattern,
                "model_id": args.model_id,
            }]

        # Get dataset list
        if args.all:
            all_datasets = manager.get_all_datasets_cached()
            dataset_names = [
                ds.get("content", {}).get("name", "").strip()
                for ds in all_datasets
                if ds.get("content", {}).get("name")
            ]
        else:
            dataset_names = args.datasets

        datasets_processed, success, failures = manager.populate_models(
            dataset_names=dataset_names,
            dataset_prefix=args.prefix,
            model_configs=model_configs,
            is_ieeg_sidecar=args.ieeg_sidecar
        )

        if failures > 0:
            sys.exit(1)


if __name__ == '__main__':
    main()
