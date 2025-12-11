from typing import Dict, Any, List, Tuple
from .base import TSVSidecar


class ParticipantsSideCarTSV(TSVSidecar):
    """
    Represents the participants.tsv sidecar file.

    - Defines required fields and defaults for each participant row
    - Allows user overrides for any field per row
    - Each dict in data corresponds to one row (participant)
    """

    filename = "participants.tsv"
    file_format = "tsv"

    REQUIRED_FIELDS = {"participant_id", "species", "age", "population", "sex", "handedness"}
    RECOMMENDED_FIELDS = set()
    OPTIONAL_FIELDS = set()

    # Default values for participant rows - user data overrides these
    ROW_DEFAULTS = {
        "species": "Homo sapiens",
        "age": "n/a",
        "population": "n/a",
        "sex": "n/a",
        "handedness": "n/a",
    }

    def __init__(self, rows: List[Dict[str, Any]] = None, **kwargs):
        """
        Initialize ParticipantsSideCarTSV with optional row data.

        Args:
            rows: List of dicts, each representing a participant row.
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
        """Add a single participant row, merging with defaults."""
        merged_row = {**self.ROW_DEFAULTS, **row}
        self.rows.append(merged_row)

    def validate(self, data: List[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate the participants.tsv structure.

        Args:
            data: Optional list of row dicts. If None, uses self.rows.

        - Ensures required columns are present.
        - Warns about missing recommended fields.
        - Warns about unexpected columns.
        - Ensures all rows contain consistent keys.
        """
        errors, warnings = [], []

        # Use provided data or fall back to self.rows
        data = data if data is not None else self.rows

        if not isinstance(data, list) or not data:
            return False, {"errors": ["Data must be a non-empty list of dictionaries."]}

        # Gather all columns found in the data
        all_fields = set().union(*(row.keys() for row in data))

        # Validation checks
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

        # Ensure all rows have the same keys
        expected_cols = list(all_fields)
        for i, row in enumerate(data):
            if set(row.keys()) != all_fields:
                warnings.append(f"Row {i+1} has inconsistent columns")

        ok = not errors
        return ok, {"errors": errors, "warnings": warnings, "columns": expected_cols}
