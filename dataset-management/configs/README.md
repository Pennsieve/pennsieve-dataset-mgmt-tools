# Model Population Configs

This folder contains configuration files for `model_populator.py`. Each config defines how to populate a metadata model from data sources.

## Config Structure

```json
{
  "org_id": "N:organization:xxx",
  "template_id": "template-uuid",
  "model_name": "model_name",
  "display_name": "Display Name",

  "sources": {
    "source_name": {
      "type": "pennsieve|local",
      "file_pattern": "filename.tsv",
      "path": "/local/path.csv"
    }
  },

  "join_key": "participant_id",

  "mappings": {
    "model_property": {"source": "source_name", "column": "column_name"},
    "static_property": {"value": "static_value"}
  }
}
```

## Fields

| Field | Description |
|-------|-------------|
| `org_id` | Pennsieve organization ID |
| `template_id` | Template UUID to create model from |
| `model_name` | Internal model name (snake_case) |
| `display_name` | Human-readable model name |
| `sources` | Data sources (Pennsieve files or local files) |
| `join_key` | Column to join records across sources |
| `mappings` | Map model properties to source columns or static values |

## Source Types

**Pennsieve source** - File from the dataset:
```json
"participants": {
  "type": "pennsieve",
  "file_pattern": "participants.tsv"
}
```

**Local source** - File on your machine:
```json
"extra_data": {
  "type": "local",
  "path": "/path/to/extra_scores.csv"
}
```

## Mapping Types

**From source column:**
```json
"participant_id": {"source": "participants", "column": "participant_id"}
```

**Column rename:**
```json
"gender": {"source": "participants", "column": "sex"}
```

**Static value:**
```json
"species": {"value": "homo sapiens"}
```

## Record Key Field

Each template defines a **key field** (marked with `x-pennsieve-key: true` in the schema) that uniquely identifies records. The populator auto-detects this field.

You must include a mapping for the key field to specify where its value comes from:

```json
"mappings": {
  "participant_id": {"source": "participants", "column": "participant_id"},
  ...
}
```

If your source column has a different name than the key field:

```json
// Template key field is "participant_id", but source column is "subject_id"
"participant_id": {"source": "participants", "column": "subject_id"}
```

## Generating a Config

Generate a starter config from a template schema:

```bash
python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --org-id "N:organization:xxx" \
    --template-id "template-uuid" \
    --generate-config \
    --output configs/my_config.json
```

## Usage

```bash
python model_populator.py --api-key <KEY> --api-secret <SECRET> \
    --config configs/participants_config.json \
    --datasets "My Dataset" \
    --dry-run
```
