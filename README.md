# Pennsieve Tools

A collection of Python tools for managing Pennsieve datasets, including metadata management, file uploads, and BIDS sidecar generation.

## Project Structure

```
pennsieve-tools/
├── shared/                     # Shared utilities (auth, config, helpers)
├── dataset-management/         # Dataset management tools
│   ├── dataset_manager/        # CLI package for dataset operations
│   ├── metadata_manager.py     # Metadata model management
│   └── pennsieve_upload.py     # File upload utilities
├── sidecar-generation/         # BIDS sidecar file generation
│   ├── sidecars/               # Sidecar class definitions
│   ├── channels_processor.py   # Channel data processing
│   └── main.py                 # Main sidecar generator
└── README.md
```

## Requirements

- Python 3.10+
- `requests` library
- `jsonschema` library (for sidecar validation)

## Authentication

All tools require Pennsieve API credentials:
- `--api-key`: Your Pennsieve API key
- `--api-secret`: Your Pennsieve API secret

---

## Dataset Manager

The `dataset_manager` CLI provides comprehensive dataset management capabilities.

### Basic Usage

```bash
cd dataset-management

python -m dataset_manager --api-key <API_KEY> --api-secret <API_SECRET> [OPTIONS]
```

### Target Datasets

You can target datasets in three ways:

```bash
# Single dataset (by name)
--datasets "My Dataset Name"

# Multiple datasets
--datasets "Dataset One" "Dataset Two" "Dataset Three"

# All datasets in your organization
--all
```

### Available Operations

#### Update Subtitle

```bash
# Single dataset
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --subtitle "A brief subtitle for the dataset"

# Multiple datasets
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "Dataset One" "Dataset Two" \
    --subtitle "Shared subtitle"

# All datasets
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --all \
    --subtitle "Organization-wide subtitle"
```

#### Update Description (README)

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --readme "This is a detailed description of the dataset contents and purpose."
```

#### Manage Tags

```bash
# Set tags (replaces existing)
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --tags epilepsy ieeg human

# Add tags (keeps existing)
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --add-tags newtag1 newtag2

# Remove specific tags
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --remove-tags oldtag
```

#### Set License

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --license "Creative Commons Attribution"
```

#### Set Banner Image

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --banner "/path/to/image.jpg"
```

#### Rename Dataset

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "Old Dataset Name" \
    --name "New Dataset Name"
```

#### Manage Collaborators

```bash
# Add a team
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --add-team "N:team:uuid-here" \
    --add-team-role editor

# Remove a team
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --remove-team "N:team:uuid-here"

# Add a user
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --add-user "N:user:uuid-here" \
    --add-user-role viewer

# Remove a user
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --remove-user "N:user:uuid-here"
```

Roles: `viewer`, `editor`, `manager`

#### Manage Contributors

```bash
# Add contributors by ID
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --contributors 1 2 3

# Remove contributors by ID
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --remove-contributors 1 2
```

#### Manage References

```bash
# Add a reference
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --add-reference "https://doi.org/10.1234/example" \
    --reference-type "IsDerivedFrom"

# Remove a reference
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --remove-reference "https://doi.org/10.1234/example"
```

#### Delete Files

Delete files from datasets by pattern or specific path. Always use `--dry-run` first to preview what will be deleted.

```bash
# Delete by glob pattern (matches filename or full path)
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --delete-pattern "*.tsv" \
    --dry-run

# Delete all JSON sidecar files
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --delete-pattern "*_ieeg.json"

# Delete files in a specific subfolder
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --delete-pattern "ieeg/*.tsv"

# Delete specific files by path
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --delete-path "ieeg/sub-001_channels.tsv" "README.txt"

# Delete from multiple datasets
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "Dataset 1" "Dataset 2" \
    --delete-pattern "*.tsv" \
    --dry-run
```

**Pattern Examples:**
| Pattern | Matches |
|---------|---------|
| `*.tsv` | All TSV files (any location) |
| `*_ieeg.json` | Files ending in `_ieeg.json` |
| `ieeg/*.tsv` | TSV files in the `ieeg` folder |
| `sub-*/*_channels.tsv` | Channel files in subject folders |

#### Cleanup Duplicate Packages

When files are re-uploaded to Pennsieve, duplicates get a `(1)` suffix. This operation finds these duplicates, deletes the original, and renames the duplicate to the original name.

```bash
# Clean up specific duplicate files
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --cleanup-duplicates participants.tsv "ieeg/channels.tsv" \
    --dry-run

# Use {dataset} placeholder for dataset-named paths
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "PennEPI00001" "PennEPI00002" \
    --cleanup-duplicates "sub-{dataset}/sub-{dataset}_sessions.tsv"
```

### Combined Operations

You can combine multiple operations in a single command:

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My Dataset" \
    --subtitle "Dataset subtitle" \
    --readme "Full description here" \
    --tags tag1 tag2 tag3 \
    --license "CC-BY-4.0" \
    --banner "/path/to/banner.jpg"
```

### Additional Flags

| Flag | Description |
|------|-------------|
| `--api-host` | Custom API host (default: https://api.pennsieve.io) |
| `--dry-run` | Preview changes without executing |
| `--force-reload` | Bypass cache and fetch fresh data |
| `--verbose` | Enable verbose logging |

---

## Pennsieve Upload

Upload files and directories to Pennsieve datasets using the Pennsieve CLI.

**Prerequisites:** The Pennsieve CLI must be installed and authenticated (`pennsieve whoami` to verify).

### Simple Upload Mode

Upload the same file or folder to one or more datasets:

```bash
cd dataset-management

# Upload a file to a single dataset
python pennsieve_upload.py --datasets "My Dataset" --path /path/to/file.json

# Upload a folder to a single dataset
python pennsieve_upload.py --datasets "My Dataset" --path /path/to/folder

# Upload to multiple datasets
python pennsieve_upload.py --datasets "Dataset 1" "Dataset 2" --path /path/to/data

# Filter by file pattern
python pennsieve_upload.py --datasets "My Dataset" --path /path/to/data --pattern .tsv .json

# Dry run (preview what would happen)
python pennsieve_upload.py --datasets "My Dataset" --path /path/to/data --dry-run
```

### Named Upload Mode

Match folder names to dataset names. Each subfolder in the source directory is uploaded to a dataset with the same name:

```bash
# Given structure:
# /path/to/output/
#   ├── PennEPI00001/
#   │   └── files...
#   ├── PennEPI00002/
#   │   └── files...
#   └── PennEPI00003/
#       └── files...

# Upload all folders to matching datasets
python pennsieve_upload.py --source-dir /path/to/output --match-names

# Upload only specific datasets
python pennsieve_upload.py --source-dir /path/to/output --match-names \
    --datasets "PennEPI00001" "PennEPI00002"

# With pattern filter
python pennsieve_upload.py --source-dir /path/to/output --match-names --pattern .tsv
```

### Options

| Flag | Description |
|------|-------------|
| `--path` | Path to file or directory to upload (simple mode) |
| `--datasets` | Dataset name(s) to upload to |
| `--source-dir` | Source directory containing dataset-named folders (named mode) |
| `--match-names` | Match folder names to dataset names (use with --source-dir) |
| `--pattern` | Only upload files containing these patterns in their name |
| `--dry-run` | Preview changes without uploading |
| `--verbose` | Enable verbose logging |

---

## Model Populator

Schema-driven model population for Pennsieve datasets. Creates models from templates and populates them with data from multiple sources.

### Features

- **Template-driven**: Fetches JSON schema from template and validates data
- **Multiple data sources**: Combine data from Pennsieve files and local files
- **Flexible mapping**: Map source columns to model properties (direct, rename, or static values)
- **Type transformation**: Automatically converts data types to match schema
- **Validation**: Checks required fields and enum values, skips invalid records

### Generate a Config Template

Generate a starter config file from a template schema:

```bash
cd dataset-management

python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --org-id "N:organization:xxx" \
    --template-id "abc-123-def" \
    --generate-config \
    --output configs/my_model_config.json
```

### Config File Structure

```json
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
    "extra_data": {
      "type": "local",
      "path": "/path/to/extra_scores.csv"
    }
  },

  "join_key": "participant_id",

  "mappings": {
    "participant_id": {"source": "participants", "column": "participant_id"},
    "sex": {"source": "participants", "column": "sex"},
    "species": {"value": "homo sapiens"},
    "custom_score": {"source": "extra_data", "column": "score"}
  }
}
```

**Mapping types:**
- `{"source": "name", "column": "col"}` - Get value from a source file column
- `{"value": "static"}` - Use a static value for all records

### Populate a Model

```bash
# Dry run first (preview without changes)
python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --config configs/participants_config.json \
    --datasets "My Dataset" \
    --dry-run \
    --verbose

# Run for real
python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --config configs/participants_config.json \
    --datasets "My Dataset"

# Multiple datasets
python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --config configs/participants_config.json \
    --datasets "Dataset 1" "Dataset 2" "Dataset 3"

# All datasets matching a prefix
python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --config configs/participants_config.json \
    --prefix PennEPI
```

### Validation Behavior

- **Required fields**: Records missing required fields are skipped with a warning
- **Enum values**: Values not in the schema's enum are excluded from the record
- **Type conversion**: Values are automatically converted (e.g., string "123" → number 123)
- **n/a handling**: `"n/a"` is only included if explicitly allowed in the schema

If a required field has an invalid value (e.g., `sex: "n/a"` when only `["Male", "Female"]` are allowed), the entire record is skipped.

---

## Metadata Manager

Manage metadata models on datasets (list, delete, legacy populate).

### List Models

```bash
cd dataset-management

# List models in a dataset
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    list --datasets "My Dataset"

# List models in multiple datasets
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    list --datasets "Dataset 1" "Dataset 2"

# List models in datasets matching a prefix
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    list --prefix PennEPI
```

### Delete Models

```bash
# Preview deletion (dry run)
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    delete --datasets "My Dataset" --dry-run

# Delete ALL models from a dataset
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    delete --datasets "My Dataset" --execute

# Delete specific models only
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    delete --datasets "My Dataset" --models person eeg participants --execute

# Delete from multiple datasets
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    delete --datasets "Dataset 1" "Dataset 2" --execute

# Delete from all datasets matching a prefix
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    delete --prefix PennEPI --models old_model --execute
```

### Legacy Populate (Simple)

For simple 1:1 file-to-model population without column mapping:

```bash
python metadata_manager.py --api-key <KEY> --api-secret <SECRET> \
    populate --datasets "My Dataset" \
    --file-pattern "_ieeg.json" \
    --template-id <TEMPLATE_UUID> \
    --model-name "bids_ieeg_sidecar" \
    --display-name "BIDS iEEG Sidecar"
```

For more complex population with column mapping, use `model_populator.py` instead.

---

## Sidecar Generation

Generate BIDS-compliant sidecar files for iEEG datasets.

### Usage

```bash
cd sidecar-generation

# Generate channels.tsv files
python channels_processor.py

# Force reload (bypass cache)
python channels_processor.py --force-reload

# Run main sidecar generator
python main.py
```

### Available Sidecar Classes

```python
from sidecars import (
    ChannelsSidecar,      # channels.tsv
    ElectrodesSidecar,    # electrodes.tsv
    EventsSidecar,        # events.tsv
    SessionSidecar,       # sessions.tsv
    ParticipantsSideCarTSV,  # participants.tsv
    IeegSidecar,          # *_ieeg.json
    EEGSidecar,           # *_eeg.json
    CoordSystemSidecar,   # *_coordsystem.json
    DatasetDescriptionSidecar,  # dataset_description.json
    ParticipantsSidecar,  # participants.json
)
```

---

## Environment Variables

You can set these environment variables instead of using command-line flags:

| Variable | Description |
|----------|-------------|
| `PENNSIEVE_API_HOST` | API host URL |
| `PENNSIEVE_CACHE_DIR` | Cache directory for API responses |
| `PENNSIEVE_OUTPUT_DIR` | Output directory for generated files |
| `MASTER_CSV_PATH` | Path to master CSV for sidecar generation |

---

## Examples

### Update multiple datasets with common metadata

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "PennEPI00001" "PennEPI00002" "PennEPI00003" \
    --tags epilepsy ieeg bids \
    --license "CC-BY-4.0"
```

### Apply settings to all datasets

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --all \
    --add-tags "epilepsy.science" \
    --add-team "N:team:your-team-id" \
    --add-team-role viewer
```

### Full dataset setup

```bash
python -m dataset_manager --api-key <KEY> --api-secret <SECRET> \
    --datasets "My New Dataset" \
    --subtitle "Human iEEG recordings for epilepsy research" \
    --readme "This dataset contains intracranial EEG recordings from patients undergoing evaluation for epilepsy surgery." \
    --tags epilepsy ieeg human adult \
    --license "CC-BY-4.0" \
    --banner "/path/to/banner.jpg" \
    --contributors 1 2 3
```
