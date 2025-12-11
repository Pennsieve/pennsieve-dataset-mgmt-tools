#!/usr/bin/env python3
"""
Model Populator

Schema-driven model population for Pennsieve datasets.

Features:
- Fetches template schema from API and validates data against it
- Supports multiple data sources (Pennsieve files + local files)
- Flexible property mapping (direct, rename, static values)
- Automatic type transformation based on JSON schema
- Joins data from multiple sources on a key column

Usage:
  # Using a config file
  python model_populator.py --api-key KEY --api-secret SECRET \
      --config population_config.json \
      --datasets PennEPI00001 \
      --dry-run

  # Interactive mode - generates config from template
  python model_populator.py --api-key KEY --api-secret SECRET \
      --org-id "N:organization:xxx" \
      --template-id "abc-123" \
      --generate-config

Example config file (population_config.json):
{
  "org_id": "N:organization:fecf73c8-b590-47fa-8de0-74cfb57051a2",
  "template_id": "8b0f84a5-fb58-4c3f-a886-ad3498e55c5b",
  "model_name": "pennepi_participants",
  "display_name": "PennEPI Participants",

  "sources": {
    "participants": {
      "type": "pennsieve",
      "file_pattern": "participants.tsv"
    },
    "extra": {
      "type": "local",
      "path": "/path/to/extra_data.csv"
    }
  },

  "join_key": "participant_id",

  "mappings": {
    "participant_id": {"source": "participants", "column": "participant_id"},
    "sex": {"source": "participants", "column": "sex"},
    "species": {"value": "homo sapiens"},
    "fiveSenseScore": {"source": "extra", "column": "five_sense"}
  }
}
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

# Set up import paths
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir))
sys.path.insert(1, str(_this_dir.parent))

from shared.config import API_HOST
from shared.auth import PennsieveAuth
from shared.helpers import load_data, save_data

API2_BASE_URL = "https://api2.pennsieve.io"


class ModelPopulator:
    """Schema-driven model population for Pennsieve datasets."""

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
        self._template_cache: Dict[str, Dict] = {}

    def _log(self, message: str, indent: int = 0):
        """Print log message with optional indentation."""
        prefix = "  " * indent
        print(f"{prefix}{message}")

    def _debug(self, message: str, indent: int = 0):
        """Print debug message if verbose mode is on."""
        if self.verbose:
            self._log(f"[DEBUG] {message}", indent)

    # -------------------------------------------------------------------------
    # Template & Schema Operations
    # -------------------------------------------------------------------------

    def fetch_templates(self, org_id: str) -> List[Dict]:
        """Fetch all templates for an organization."""
        encoded_org_id = quote(org_id, safe="")
        url = f"{API2_BASE_URL}/metadata/templates?organization_id={encoded_org_id}"

        self._debug(f"Fetching templates from: {url}")

        response = requests.get(url, headers=self.auth.get_headers())
        response.raise_for_status()

        return response.json()

    def get_template_schema(self, org_id: str, template_id: str) -> Optional[Dict]:
        """Get the JSON schema for a specific template."""
        cache_key = f"{org_id}:{template_id}"
        if cache_key in self._template_cache:
            return self._template_cache[cache_key]

        templates = self.fetch_templates(org_id)

        for item in templates:
            template = item.get("model_template", {})
            if template.get("id") == template_id:
                schema = template.get("latest_version", {}).get("schema", {})
                self._template_cache[cache_key] = schema
                return schema

        return None

    def get_schema_properties(self, schema: Dict) -> Dict[str, Dict]:
        """Extract properties from a JSON schema."""
        return schema.get("properties", {})

    def get_required_fields(self, schema: Dict) -> List[str]:
        """Get required fields from schema."""
        return schema.get("required", [])

    def get_key_field(self, schema: Dict) -> Optional[str]:
        """Find the field marked as x-pennsieve-key."""
        properties = self.get_schema_properties(schema)
        for prop_name, prop_def in properties.items():
            if prop_def.get("x-pennsieve-key"):
                return prop_name
        return None

    # -------------------------------------------------------------------------
    # Data Source Operations
    # -------------------------------------------------------------------------

    def get_all_datasets(self) -> List[Dict]:
        """Fetch all datasets with caching."""
        datasets = load_data("datasets", force_reload=self.force_reload)
        if datasets is None:
            self._log("Fetching datasets from network...")
            datasets = self._fetch_all_datasets()
            save_data(datasets, "datasets")
        return datasets

    def _fetch_all_datasets(self) -> List[Dict]:
        """Fetch all datasets via API."""
        datasets = []
        offset = 0
        page_size = 25

        while True:
            url = (
                f"{API_HOST}/datasets/paginated"
                f"?limit={page_size}&offset={offset}"
                f"&orderBy=Name&orderDirection=Asc"
            )

            response = requests.get(url, headers=self.auth.get_headers())
            response.raise_for_status()

            result = response.json()
            batch = result.get("datasets", [])
            if not batch:
                break

            datasets.extend(batch)
            total_count = result.get("totalCount", 0)
            offset += page_size
            if offset >= total_count:
                break

        return datasets

    def find_dataset_by_name(self, name: str) -> Optional[Dict]:
        """Find a dataset by name."""
        datasets = self.get_all_datasets()
        for ds in datasets:
            if ds.get("content", {}).get("name") == name:
                return ds
        return None

    def get_dataset_packages(self, dataset_id: str) -> List[Dict]:
        """Get all packages for a dataset."""
        cache_key = f"packages_{dataset_id.replace(':', '_')}"
        packages = load_data(cache_key, force_reload=self.force_reload)

        if packages is not None:
            return packages

        self._log(f"Fetching packages for dataset...")
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

        save_data(all_packages, cache_key)
        return all_packages

    def get_package_path(self, package: Dict, all_packages: List[Dict]) -> str:
        """Get the folder path for a package."""
        pkg_lookup = {
            pkg.get("content", {}).get("id"): pkg
            for pkg in all_packages
        }

        path_parts = []
        current = package

        while True:
            content = current.get("content", {})
            parent_id = content.get("parentId")

            if not parent_id or parent_id not in pkg_lookup:
                break

            parent = pkg_lookup[parent_id]
            parent_name = parent.get("content", {}).get("name", "")
            if parent_name:
                path_parts.insert(0, parent_name)
            current = parent

        return "/".join(path_parts)

    def find_file_in_dataset(
        self,
        packages: List[Dict],
        file_pattern: str
    ) -> Optional[Dict]:
        """Find a file matching a pattern in dataset packages."""
        for pkg in packages:
            content = pkg.get("content", {})
            name = content.get("name", "")

            if name.startswith("__DELETED__"):
                continue

            pkg_path = self.get_package_path(pkg, packages)
            if "archive" in pkg_path.lower():
                continue

            if name == file_pattern or name.endswith(file_pattern):
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

    def load_local_file(self, path: str) -> List[Dict[str, Any]]:
        """Load data from a local CSV/TSV/JSON file."""
        file_path = Path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"Local file not found: {path}")

        content = file_path.read_text()
        filename = file_path.name

        return self._parse_file_content(content, filename)

    def _parse_file_content(self, content: str, filename: str) -> List[Dict[str, Any]]:
        """Parse file content into records."""
        if filename.endswith('.json'):
            data = json.loads(content)
            return [data] if isinstance(data, dict) else data
        elif filename.endswith('.csv') or filename.endswith('.tsv'):
            return self._csv_to_records(content)
        else:
            # Try CSV first, then JSON
            try:
                return self._csv_to_records(content)
            except Exception:
                return json.loads(content)

    def _csv_to_records(self, content: str) -> List[Dict[str, Any]]:
        """Convert CSV/TSV content to records."""
        dialect = csv.Sniffer().sniff(content[:1024], delimiters=',\t')
        reader = csv.DictReader(io.StringIO(content), dialect=dialect)
        return list(reader)

    # -------------------------------------------------------------------------
    # Data Transformation
    # -------------------------------------------------------------------------

    def transform_value(self, value: Any, prop_schema: Dict) -> Any:
        """
        Transform a value to match the schema type.

        Returns None if the value doesn't conform to the schema (will be excluded from record).
        """
        if value is None or value == "" or value == "n/a":
            # Check if "n/a" is explicitly allowed
            if value == "n/a":
                # Check oneOf for "n/a" enum
                if "oneOf" in prop_schema:
                    for option in prop_schema["oneOf"]:
                        if "enum" in option and "n/a" in option["enum"]:
                            return "n/a"
                # Check direct enum
                if "enum" in prop_schema and "n/a" in prop_schema["enum"]:
                    return "n/a"
                # n/a not allowed, exclude field
                return None
            return None

        # Handle oneOf (e.g., number OR "n/a")
        if "oneOf" in prop_schema:
            for option in prop_schema["oneOf"]:
                # Check enum option
                if "enum" in option:
                    if value in option["enum"]:
                        return value
                    # Case-insensitive match
                    for enum_val in option["enum"]:
                        if str(value).lower() == str(enum_val).lower():
                            return enum_val
                # Check type option
                elif "type" in option:
                    try:
                        transformed = self._transform_to_type(value, option)
                        if transformed is not None:
                            return transformed
                    except (ValueError, TypeError):
                        continue
            # No option matched, exclude field
            self._debug(f"Value '{value}' doesn't match any oneOf option, excluding")
            return None

        # Handle enum
        if "enum" in prop_schema:
            if value in prop_schema["enum"]:
                return value
            # Try case-insensitive match
            for enum_val in prop_schema["enum"]:
                if str(value).lower() == str(enum_val).lower():
                    return enum_val
            # Value not in enum, exclude field
            self._debug(f"Value '{value}' not in enum {prop_schema['enum']}, excluding")
            return None

        # Handle type
        prop_type = prop_schema.get("type")
        if prop_type:
            return self._transform_to_type(value, prop_schema)

        return value

    def _transform_to_type(self, value: Any, schema: Dict) -> Any:
        """Transform value to match schema type."""
        prop_type = schema.get("type")

        # Handle array of types (e.g., ["string", "null"])
        if isinstance(prop_type, list):
            for t in prop_type:
                if t == "null" and (value is None or value == ""):
                    return None
                try:
                    return self._convert_to_type(value, t)
                except (ValueError, TypeError):
                    continue
            return value

        return self._convert_to_type(value, prop_type)

    def _convert_to_type(self, value: Any, type_name: str) -> Any:
        """Convert value to a specific type."""
        if type_name == "string":
            return str(value) if value is not None else None
        elif type_name == "number":
            if value == "n/a" or value == "":
                return value
            return float(value)
        elif type_name == "integer":
            if value == "n/a" or value == "":
                return value
            return int(float(value))
        elif type_name == "boolean":
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        elif type_name == "object":
            if isinstance(value, dict):
                return value
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        elif type_name == "array":
            if isinstance(value, list):
                return value
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return [value]
            return [value]
        elif type_name == "null":
            return None

        return value

    def build_record(
        self,
        mappings: Dict[str, Dict],
        source_data: Dict[str, List[Dict]],
        schema: Dict,
        row_index: int = 0,
        join_key: Optional[str] = None,
        join_value: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build a single record from mappings and source data."""
        properties = self.get_schema_properties(schema)
        record = {}

        for prop_name, prop_schema in properties.items():
            if prop_name not in mappings:
                # Check for default value in schema
                if "default" in prop_schema:
                    record[prop_name] = prop_schema["default"]
                continue

            mapping = mappings[prop_name]
            value = None

            # Static value
            if "value" in mapping:
                value = mapping["value"]

            # From source
            elif "source" in mapping and "column" in mapping:
                source_name = mapping["source"]
                column_name = mapping["column"]

                if source_name in source_data:
                    source_records = source_data[source_name]

                    # Find the right record
                    if join_key and join_value:
                        for src_record in source_records:
                            if src_record.get(join_key) == join_value:
                                value = src_record.get(column_name)
                                break
                    elif row_index < len(source_records):
                        value = source_records[row_index].get(column_name)

            # Transform value to match schema (only include if valid)
            if value is not None and value != "":
                transformed = self.transform_value(value, prop_schema)
                if transformed is not None:
                    record[prop_name] = transformed

        return record

    # -------------------------------------------------------------------------
    # Model Operations
    # -------------------------------------------------------------------------

    def get_existing_model(self, dataset_id: str, model_name: str) -> Optional[str]:
        """Find existing model by name, return its ID."""
        encoded_id = quote(dataset_id, safe="")
        url = f"{API2_BASE_URL}/metadata/models?dataset_id={encoded_id}"

        response = requests.get(url, headers=self.auth.get_headers())
        response.raise_for_status()

        models = response.json()
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
        description: str = ""
    ) -> Optional[str]:
        """Create a model from template."""
        encoded_id = quote(dataset_id, safe="")
        url = f"{API2_BASE_URL}/metadata/templates/{template_id}/models?dataset_id={encoded_id}&version=1"

        payload = {
            "name": model_name,
            "display_name": display_name,
            "description": description
        }

        self._debug(f"Creating model: {url}")
        self._debug(f"Payload: {json.dumps(payload, indent=2)}")

        if self.dry_run:
            self._log("[DRY-RUN] Would create model")
            return "dry-run-model-id"

        response = requests.post(url, json=payload, headers=self.auth.get_headers())

        # Handle duplicate model
        if response.status_code == 400:
            try:
                error = response.json()
                if "duplicate model name" in error.get("message", ""):
                    self._log("Model already exists, using existing...")
                    return self.get_existing_model(dataset_id, model_name)
            except json.JSONDecodeError:
                pass

        response.raise_for_status()

        result = response.json()
        return result.get("model", {}).get("id")

    def post_records(
        self,
        model_id: str,
        dataset_id: str,
        records: List[Dict]
    ) -> bool:
        """Post records to a model."""
        encoded_id = quote(dataset_id, safe="")
        url = f"{API2_BASE_URL}/metadata/models/{model_id}/records?dataset_id={encoded_id}"

        payload = {"records": records}

        self._debug(f"Posting {len(records)} records to: {url}")

        if self.dry_run:
            self._log(f"[DRY-RUN] Would post {len(records)} records")
            self._log(f"[DRY-RUN] Sample record: {json.dumps(records[0] if records else {}, indent=2)}")
            return True

        response = requests.post(url, json=payload, headers=self.auth.get_headers())

        if not response.ok:
            self._log(f"ERROR: {response.status_code}")
            self._log(f"Response: {response.text}")
            return False

        response.raise_for_status()
        self._log(f"Posted {len(records)} records successfully")
        return True

    # -------------------------------------------------------------------------
    # Main Population Logic
    # -------------------------------------------------------------------------

    def populate_dataset(
        self,
        dataset_name: str,
        config: Dict
    ) -> bool:
        """Populate a model in a dataset using the config."""
        self._log(f"\n{'='*60}")
        self._log(f"Processing dataset: {dataset_name}")
        self._log(f"{'='*60}")

        # Find dataset
        dataset = self.find_dataset_by_name(dataset_name)
        if not dataset:
            self._log(f"ERROR: Dataset not found: {dataset_name}")
            return False

        dataset_id = dataset.get("content", {}).get("id")
        self._log(f"Dataset ID: {dataset_id}", indent=1)

        # Get template schema
        org_id = config["org_id"]
        template_id = config["template_id"]

        self._log(f"\nStep 1: Fetching template schema...", indent=1)
        schema = self.get_template_schema(org_id, template_id)
        if not schema:
            self._log(f"ERROR: Template not found: {template_id}")
            return False

        properties = self.get_schema_properties(schema)
        self._log(f"Schema has {len(properties)} properties", indent=2)

        key_field = self.get_key_field(schema)
        if key_field:
            self._log(f"Key field: {key_field}", indent=2)

        # Load data sources
        self._log(f"\nStep 2: Loading data sources...", indent=1)
        source_data: Dict[str, List[Dict]] = {}
        packages = None

        for source_name, source_config in config.get("sources", {}).items():
            source_type = source_config.get("type")

            if source_type == "pennsieve":
                if packages is None:
                    packages = self.get_dataset_packages(dataset_id)

                file_pattern = source_config.get("file_pattern")
                pkg = self.find_file_in_dataset(packages, file_pattern)

                if not pkg:
                    self._log(f"WARNING: File not found: {file_pattern}", indent=2)
                    continue

                node_id = pkg.get("content", {}).get("nodeId")
                filename = pkg.get("content", {}).get("name")
                self._log(f"Found {source_name}: {filename}", indent=2)

                if not self.dry_run:
                    content = self.download_file_content(node_id)
                    records = self._parse_file_content(content, filename)
                    source_data[source_name] = records
                    self._log(f"Loaded {len(records)} records from {source_name}", indent=3)
                else:
                    self._log(f"[DRY-RUN] Would download {filename}", indent=3)
                    source_data[source_name] = []

            elif source_type == "local":
                path = source_config.get("path")
                self._log(f"Loading local file: {path}", indent=2)

                try:
                    records = self.load_local_file(path)
                    source_data[source_name] = records
                    self._log(f"Loaded {len(records)} records from {source_name}", indent=3)
                except FileNotFoundError as e:
                    self._log(f"WARNING: {e}", indent=2)

        # Build records
        self._log(f"\nStep 3: Building records...", indent=1)
        mappings = config.get("mappings", {})
        join_key = config.get("join_key")

        # Determine record count from primary source
        primary_source = list(source_data.keys())[0] if source_data else None
        primary_records = source_data.get(primary_source, [])

        if not primary_records and not self.dry_run:
            self._log("ERROR: No source data available")
            return False

        required_fields = self.get_required_fields(schema)
        self._log(f"Required fields: {required_fields}", indent=2)

        records = []
        skipped = []
        for i, primary_record in enumerate(primary_records):
            join_value = primary_record.get(join_key) if join_key else None

            record = self.build_record(
                mappings=mappings,
                source_data=source_data,
                schema=schema,
                row_index=i,
                join_key=join_key,
                join_value=join_value
            )

            # Check required fields
            missing_required = [f for f in required_fields if f not in record]
            if missing_required:
                record_id = record.get(key_field, f"row {i}")
                self._log(f"WARNING: Record '{record_id}' missing required fields: {missing_required}", indent=2)
                skipped.append((record_id, missing_required))
                continue

            if record:
                records.append(record)

        self._log(f"Built {len(records)} valid records", indent=2)
        if skipped:
            self._log(f"Skipped {len(skipped)} records with missing required fields", indent=2)

        if records and self.verbose:
            self._log(f"Sample record:", indent=2)
            self._log(json.dumps(records[0], indent=2), indent=3)

        # Create/find model
        self._log(f"\nStep 4: Creating model...", indent=1)
        model_name = config.get("model_name")
        display_name = config.get("display_name", model_name)

        model_id = self.create_model_from_template(
            template_id=template_id,
            dataset_id=dataset_id,
            model_name=model_name,
            display_name=display_name
        )

        if not model_id:
            self._log("ERROR: Failed to create model")
            return False

        self._log(f"Model ID: {model_id}", indent=2)

        # Post records
        self._log(f"\nStep 5: Posting records...", indent=1)
        if records or self.dry_run:
            success = self.post_records(model_id, dataset_id, records)
            if not success:
                return False

        self._log(f"\nSUCCESS: Completed {dataset_name}")
        return True

    def generate_config_template(
        self,
        org_id: str,
        template_id: str,
        output_path: Optional[str] = None
    ) -> Dict:
        """Generate a config template from a schema."""
        self._log(f"Fetching template schema...")

        schema = self.get_template_schema(org_id, template_id)
        if not schema:
            raise ValueError(f"Template not found: {template_id}")

        properties = self.get_schema_properties(schema)
        title = schema.get("title", "model")

        # Build mappings with placeholders
        mappings = {}
        for prop_name, prop_schema in properties.items():
            description = prop_schema.get("description", "")
            prop_type = prop_schema.get("type", "string")

            # Try to infer source from description
            if "participants.tsv" in description.lower():
                mappings[prop_name] = {
                    "source": "participants",
                    "column": prop_name,
                    "_comment": description
                }
            elif "ieeg.json" in description.lower():
                mappings[prop_name] = {
                    "source": "ieeg",
                    "column": prop_name,
                    "_comment": description
                }
            elif "sessions.tsv" in description.lower():
                mappings[prop_name] = {
                    "source": "sessions",
                    "column": prop_name,
                    "_comment": description
                }
            elif "default" in prop_schema:
                mappings[prop_name] = {
                    "value": prop_schema["default"],
                    "_comment": f"Default value from schema"
                }
            else:
                mappings[prop_name] = {
                    "source": "TODO",
                    "column": prop_name,
                    "_comment": description or f"Type: {prop_type}"
                }

        config = {
            "org_id": org_id,
            "template_id": template_id,
            "model_name": title,
            "display_name": schema.get("title", title).replace("_", " ").title(),
            "sources": {
                "participants": {
                    "type": "pennsieve",
                    "file_pattern": "participants.tsv"
                },
                "ieeg": {
                    "type": "pennsieve",
                    "file_pattern": "_ieeg.json"
                },
                "sessions": {
                    "type": "pennsieve",
                    "file_pattern": "_sessions.tsv"
                }
            },
            "join_key": "participant_id",
            "mappings": mappings
        }

        if output_path:
            with open(output_path, 'w') as f:
                json.dump(config, f, indent=2)
            self._log(f"Config written to: {output_path}")

        return config


def main():
    parser = argparse.ArgumentParser(
        description='Schema-driven model population for Pennsieve datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Populate using a config file
  %(prog)s --api-key KEY --api-secret SECRET \\
      --config population_config.json \\
      --datasets PennEPI00001 PennEPI00002 \\
      --dry-run

  # Generate a config template from a schema
  %(prog)s --api-key KEY --api-secret SECRET \\
      --org-id "N:organization:xxx" \\
      --template-id "abc-123" \\
      --generate-config --output my_config.json

  # Populate all datasets matching a prefix
  %(prog)s --api-key KEY --api-secret SECRET \\
      --config population_config.json \\
      --prefix PennEPI
        """
    )

    # Authentication
    parser.add_argument('--api-key', required=True, help='Pennsieve API key')
    parser.add_argument('--api-secret', required=True, help='Pennsieve API secret')

    # Config
    parser.add_argument('--config', help='Path to population config JSON file')
    parser.add_argument('--org-id', help='Organization ID (for generate-config)')
    parser.add_argument('--template-id', help='Template ID (for generate-config)')

    # Dataset selection
    parser.add_argument('--datasets', nargs='+', help='Dataset names to process')
    parser.add_argument('--prefix', help='Process datasets matching this prefix')

    # Config generation
    parser.add_argument('--generate-config', action='store_true',
                        help='Generate a config template from a schema')
    parser.add_argument('--output', help='Output path for generated config')

    # Options
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    parser.add_argument('--force-reload', action='store_true', help='Bypass cache')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')

    args = parser.parse_args()

    # Validate arguments
    if args.generate_config:
        if not args.org_id or not args.template_id:
            parser.error("--generate-config requires --org-id and --template-id")
    elif not args.config:
        parser.error("--config is required unless using --generate-config")

    if not args.generate_config and not args.datasets and not args.prefix:
        parser.error("Must specify --datasets or --prefix")

    # Authenticate
    print("Authenticating...")
    auth = PennsieveAuth()
    auth.authenticate(args.api_key, args.api_secret)
    print("Authentication successful\n")

    populator = ModelPopulator(
        auth=auth,
        dry_run=args.dry_run,
        force_reload=args.force_reload,
        verbose=args.verbose
    )

    # Generate config mode
    if args.generate_config:
        config = populator.generate_config_template(
            org_id=args.org_id,
            template_id=args.template_id,
            output_path=args.output
        )
        if not args.output:
            print(json.dumps(config, indent=2))
        return

    # Population mode
    with open(args.config) as f:
        config = json.load(f)

    # Get dataset list
    if args.datasets:
        dataset_names = args.datasets
    else:
        all_datasets = populator.get_all_datasets()
        dataset_names = [
            ds.get("content", {}).get("name")
            for ds in all_datasets
            if ds.get("content", {}).get("name", "").startswith(args.prefix)
        ]

    if not dataset_names:
        print("No datasets found to process")
        sys.exit(1)

    print(f"Datasets to process: {len(dataset_names)}")

    if args.dry_run:
        print("\n" + "="*60)
        print("DRY RUN MODE - No changes will be made")
        print("="*60)

    # Process datasets
    success_count = 0
    fail_count = 0

    for dataset_name in dataset_names:
        try:
            if populator.populate_dataset(dataset_name, config):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            print(f"ERROR processing {dataset_name}: {e}")
            fail_count += 1

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Datasets processed: {len(dataset_names)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")

    if args.dry_run:
        print("\n(Dry-run mode - no changes were made)")

    if fail_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
