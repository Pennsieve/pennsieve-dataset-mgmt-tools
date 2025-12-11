from typing import Dict, Any, List, Tuple
from .base import TSVSidecar


class ChannelsSidecar(TSVSidecar):
    """
    Represents the channels.tsv BIDS sidecar file.

    - Defines required fields and defaults for each channel row
    - Allows user overrides for any field per row
    - Validates using BIDS iEEG specification rules
    - Each row corresponds to one recorded channel
    """

    filename = "channels.tsv"
    file_format = "tsv"

    # Field definitions based on BIDS iEEG specification
    REQUIRED_FIELDS = {
        "name",
        "type",
        "units",
        "sampling_frequency",
        "low_cutoff",
        "high_cutoff",
        "notch",
        "reference",
        "group",
    }

    RECOMMENDED_FIELDS = {"ground"}
    OPTIONAL_FIELDS = {
        "description",  # free text column for optional notes
    }

    # Default values for channel rows - user data overrides these
    ROW_DEFAULTS = {
        "type": "SEEG",
        "units": "uV",
        "low_cutoff": "n/a",
        "high_cutoff": "n/a",
        "notch": "n/a",
        "reference": "unknown",
        "ground": "unknown",
        "group": "n/a",
        "sampling_frequency": "n/a",
    }

    # Column order for TSV output
    COLUMN_ORDER = [
        "name", "type", "units", "low_cutoff", "high_cutoff",
        "reference", "ground", "group", "sampling_frequency", "notch"
    ]

    def __init__(self, rows: List[Dict[str, Any]] = None, **kwargs):
        """
        Initialize ChannelsSidecar with optional row data.

        Args:
            rows: List of dicts, each representing a channel row.
                  Each row is merged with ROW_DEFAULTS (user values override defaults).
            **kwargs: Additional options passed to parent (output_dir, etc.)
        """
        # Initialize parent with empty fields dict (TSV uses row data, not fields)
        super().__init__(fields={}, **kwargs)

        # Process rows: merge each row with defaults
        self.rows = []
        if rows:
            for row in rows:
                merged_row = {**self.ROW_DEFAULTS, **row}
                self.rows.append(merged_row)

        self.log.debug(
            f"{self.__class__.__name__} initialized with {len(self.rows)} rows "
            f"(defaults: {list(self.ROW_DEFAULTS.keys())})"
        )

    def add_row(self, row: Dict[str, Any]) -> None:
        """Add a single channel row, merging with defaults."""
        merged_row = {**self.ROW_DEFAULTS, **row}
        self.rows.append(merged_row)

    def validate(self, data: List[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate the channels.tsv structure.

        Args:
            data: Optional list of row dicts. If None, uses self.rows.

        - Ensures required columns are present.
        - Warns about missing optional fields.
        - Warns about unexpected columns.
        - Ensures all rows have consistent keys.
        - Checks that numeric columns contain numeric-like values.
        """
        errors, warnings = [], []

        # Use provided data or fall back to self.rows
        data = data if data is not None else self.rows

        if not isinstance(data, list) or not data:
            return False, {"errors": ["Data must be a non-empty list of dictionaries."]}

        all_fields = set().union(*(row.keys() for row in data))

        # Field presence validation
        missing_required = self.REQUIRED_FIELDS - all_fields
        extra_fields = all_fields - (
            self.REQUIRED_FIELDS | self.RECOMMENDED_FIELDS | self.OPTIONAL_FIELDS
        )

        if missing_required:
            errors.append(f"Missing REQUIRED fields: {sorted(missing_required)}")

        if extra_fields:
            warnings.append(f"Extra (non-BIDS) fields detected: {sorted(extra_fields)}")

        # Consistency check — all rows have same fields
        for i, row in enumerate(data):
            if set(row.keys()) != all_fields:
                warnings.append(f"Row {i+1} has inconsistent columns")

        # Check numeric fields for proper format
        numeric_fields = ["low_cutoff", "high_cutoff", "sampling_frequency", "notch"]
        for field in numeric_fields:
            for i, row in enumerate(data):
                val = row.get(field, None)
                if val not in (None, "n/a", "N/A"):
                    try:
                        float(val)
                    except (TypeError, ValueError):
                        warnings.append(f"Row {i+1}: Field '{field}' should be numeric, got '{val}'")

        ok = not errors
        return ok, {"errors": errors, "warnings": warnings, "columns": sorted(all_fields)}

    def save(self, output_path: str = None, **kwargs) -> str:
        """
        Save channels.tsv to disk with proper column ordering.

        Args:
            output_path: Full path for output file. If None, uses default from parent.
            **kwargs: Additional options (validate=True to run validation first)

        Returns:
            Path to the saved file.
        """
        import os
        import csv

        data = kwargs.get("data", self.rows)

        if kwargs.get("validate", False):
            ok, report = self.validate(data)
            if not ok:
                self.log.warning(
                    f"Validation failed with {len(report.get('errors', []))} errors"
                )

        # Determine output path
        if output_path is None:
            output_dir = kwargs.get("output_dir", "output/tsv")
            output_path = os.path.join(output_dir, self.filename)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Use COLUMN_ORDER for consistent output, adding any extra columns at the end
        if data:
            all_cols = set().union(*(row.keys() for row in data))
            extra_cols = sorted(all_cols - set(self.COLUMN_ORDER))
            fieldnames = [c for c in self.COLUMN_ORDER if c in all_cols] + extra_cols
        else:
            fieldnames = self.COLUMN_ORDER

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(data)

        self.log.info(f"Saved {self.__class__.__name__} ({len(data)} rows) to {output_path}")
        return output_path
