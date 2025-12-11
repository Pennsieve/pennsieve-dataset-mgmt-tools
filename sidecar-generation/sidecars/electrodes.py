from typing import Dict, Any, List, Tuple
from .base import TSVSidecar


class ElectrodesSidecar(TSVSidecar):
    """
    Represents the electrodes.tsv BIDS sidecar file.

    - Defines required fields and defaults for each electrode row
    - Allows user overrides for any field per row
    - Each dict in data corresponds to one electrode row
    """

    filename = "electrodes.tsv"
    file_format = "tsv"

    REQUIRED_FIELDS = {"name", "x", "y", "z", "size"}
    RECOMMENDED_FIELDS = {"material", "manufacturer", "group", "hemisphere"}
    OPTIONAL_FIELDS = {"type", "impedance", "dimension", "roi"}

    # Default values for electrode rows - user data overrides these
    ROW_DEFAULTS = {
        "size": "n/a",
        "material": "n/a",
        "manufacturer": "n/a",
        "group": "n/a",
        "hemisphere": "n/a",
    }

    def __init__(self, rows: List[Dict[str, Any]] = None, **kwargs):
        """
        Initialize ElectrodesSidecar with optional row data.

        Args:
            rows: List of dicts, each representing an electrode row.
                  Each row is merged with ROW_DEFAULTS (user values override defaults).
            **kwargs: Additional options passed to parent (output_dir, etc.)
        """
        super().__init__(fields={}, **kwargs)

        self.rows = []
        if rows:
            for row in rows:
                merged_row = {**self.ROW_DEFAULTS, **row}
                self.rows.append(merged_row)

        self.log.debug(
            f"{self.__class__.__name__} initialized with {len(self.rows)} rows"
        )

    def add_row(self, row: Dict[str, Any]) -> None:
        """Add a single electrode row, merging with defaults."""
        merged_row = {**self.ROW_DEFAULTS, **row}
        self.rows.append(merged_row)

    def validate(self, data: List[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate the electrodes.tsv structure.

        Args:
            data: Optional list of row dicts. If None, uses self.rows.

        - Ensures required columns are present.
        - Warns about missing recommended fields.
        - Warns about unexpected columns.
        - Ensures all rows contain consistent keys.
        - Ensures numeric fields contain valid numbers.
        """
        errors, warnings = [], []

        # Use provided data or fall back to self.rows
        data = data if data is not None else self.rows

        if not isinstance(data, list) or not data:
            return False, {"errors": ["Data must be a non-empty list of dictionaries."]}

        # Gather all columns found in the data
        all_fields = set().union(*(row.keys() for row in data))

        # Field-level presence validation
        missing_required = self.REQUIRED_FIELDS - all_fields
        missing_recommended = self.RECOMMENDED_FIELDS - all_fields
        extra_fields = all_fields - (
            self.REQUIRED_FIELDS | self.RECOMMENDED_FIELDS | self.OPTIONAL_FIELDS
        )

        if missing_required:
            errors.append(f"Missing REQUIRED fields: {sorted(missing_required)}")
        if missing_recommended:
            warnings.append(f"Missing RECOMMENDED fields: {sorted(missing_recommended)}")
        if extra_fields:
            warnings.append(f"Extra (non-BIDS) fields detected: {sorted(extra_fields)}")

        # Consistency: all rows have same keys
        for i, row in enumerate(data):
            if set(row.keys()) != all_fields:
                warnings.append(f"Row {i+1} has inconsistent columns")

        # Check numeric fields
        numeric_fields = ["x", "y", "z", "size", "impedance"]
        for i, row in enumerate(data):
            for field in numeric_fields:
                val = row.get(field)
                if val not in (None, "n/a", "N/A"):
                    try:
                        float(val)
                    except (TypeError, ValueError):
                        errors.append(
                            f"Row {i+1}: Field '{field}' must be numeric, got '{val}'"
                        )

        ok = not errors
        return ok, {"errors": errors, "warnings": warnings, "columns": sorted(all_fields)}
