#!/usr/bin/env python3
"""
Pennsieve Upload Manager

Upload files and directories to Pennsieve datasets using the Pennsieve CLI.

Usage:
  # Upload a file or folder to a single dataset
  python pennsieve_upload.py --datasets "My Dataset" --path /path/to/file_or_folder

  # Upload to multiple datasets
  python pennsieve_upload.py --datasets "Dataset 1" "Dataset 2" --path /path/to/data

  # Upload with file pattern filter
  python pennsieve_upload.py --datasets "My Dataset" --path /path/to/data --pattern .tsv .json

  # Named upload: match folder names to dataset names
  python pennsieve_upload.py --source-dir /path/to/output --match-names

  # Named upload: specific datasets only
  python pennsieve_upload.py --source-dir /path/to/output --match-names --datasets "PennEPI00001" "PennEPI00002"

  # Dry run (preview what would happen)
  python pennsieve_upload.py --datasets "My Dataset" --path /path/to/data --dry-run
"""

import argparse
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

# Set up import paths
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir))
sys.path.insert(1, str(_this_dir.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pennsieve_upload.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PennsieveUploader:
    """Upload files and directories to Pennsieve using the CLI."""

    def __init__(self, dry_run: bool = False, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose

        if verbose:
            logger.setLevel(logging.DEBUG)

    def _run_command(self, cmd: List[str], capture_output: bool = True) -> Tuple[int, str, str]:
        """Run a shell command and return the result."""
        logger.debug(f"Running command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True
        )

        return result.returncode, result.stdout, result.stderr

    def find_dataset_node_id(self, dataset_name: str) -> Optional[str]:
        """Find the Pennsieve node ID for a dataset."""
        logger.info(f"Looking up dataset: {dataset_name}")

        returncode, stdout, stderr = self._run_command(
            ['pennsieve', 'dataset', 'find', dataset_name]
        )

        if returncode != 0:
            logger.error(f"Failed to find dataset '{dataset_name}': {stderr or stdout}")
            return None

        match = re.search(r'(N:dataset:[\w-]+)', stdout)

        if match:
            node_id = match.group(1)
            logger.info(f"  Found: {node_id}")
            return node_id
        else:
            logger.error(f"Could not parse node ID from output: {stdout}")
            return None

    def set_active_dataset(self, node_id: str) -> bool:
        """Set the active dataset for CLI operations."""
        logger.info(f"Setting active dataset: {node_id}")

        if self.dry_run:
            logger.info(f"  [DRY RUN] Would set active dataset")
            return True

        returncode, stdout, stderr = self._run_command(
            ['pennsieve', 'dataset', 'use', node_id]
        )

        if returncode != 0:
            logger.error(f"Failed to set active dataset: {stderr}")
            return False

        return True

    def get_files_to_upload(
        self,
        path: Path,
        patterns: Optional[List[str]] = None
    ) -> List[Path]:
        """
        Get list of files to upload from a path.

        Args:
            path: File or directory path
            patterns: Optional list of patterns to filter files

        Returns:
            List of file paths to upload
        """
        if path.is_file():
            return [path]

        # It's a directory - get all files recursively
        files = []
        for item in path.rglob('*'):
            if item.is_dir():
                continue

            # Skip hidden files
            if any(part.startswith('.') for part in item.parts):
                logger.debug(f"Skipping hidden file: {item}")
                continue

            # Apply pattern filter if specified
            if patterns:
                if not any(p in item.name for p in patterns):
                    continue

            files.append(item)

        return sorted(files)

    def create_manifest(self, file_path: Path, target_path: Optional[str] = None) -> Optional[int]:
        """Create a Pennsieve manifest for a file."""
        logger.info(f"Creating manifest for: {file_path}")

        if self.dry_run:
            logger.info(f"  [DRY RUN] Would create manifest (target: {target_path or 'root'})")
            return 999  # Fake manifest ID for dry run

        full_path = file_path.resolve()
        cmd = ['pennsieve', 'manifest', 'create', str(full_path)]
        if target_path and target_path != '.':
            cmd.extend(['-t', target_path])

        returncode, stdout, stderr = self._run_command(cmd)

        if returncode != 0:
            logger.error(f"Failed to create manifest: {stderr}")
            return None

        match = re.search(r'Manifest ID:\s*(\d+)', stdout)

        if match:
            manifest_id = int(match.group(1))
            logger.info(f"  Created manifest ID: {manifest_id}")
            return manifest_id
        else:
            logger.error(f"Could not parse manifest ID from output: {stdout}")
            return None

    def add_to_manifest(
        self,
        manifest_id: int,
        file_path: Path,
        base_path: Path
    ) -> bool:
        """Add a file to an existing manifest."""
        if self.dry_run:
            logger.debug(f"  [DRY RUN] Would add to manifest: {file_path}")
            return True

        full_path = file_path.resolve()

        # Calculate target path relative to base
        try:
            relative_path = file_path.relative_to(base_path)
            target_path = str(relative_path.parent)
        except ValueError:
            target_path = None

        cmd = ['pennsieve', 'manifest', 'add', str(manifest_id), str(full_path)]
        if target_path and target_path != '.':
            cmd.extend(['-t', target_path])

        returncode, stdout, stderr = self._run_command(cmd)
        if returncode != 0:
            logger.error(f"Failed to add file to manifest: {stderr}")
            return False

        return True

    def upload_manifest(self, manifest_id: int) -> bool:
        """Upload files using the specified manifest."""
        logger.info(f"Uploading manifest: {manifest_id}")

        if self.dry_run:
            logger.info(f"  [DRY RUN] Would upload manifest {manifest_id}")
            return True

        returncode, stdout, stderr = self._run_command(
            ['pennsieve', 'upload', 'manifest', str(manifest_id)],
            capture_output=True
        )

        if stdout:
            logger.info(f"Upload output: {stdout}")

        if returncode != 0:
            logger.error(f"Failed to upload manifest: {stderr}")
            return False

        logger.info(f"Successfully uploaded manifest {manifest_id}")
        return True

    def upload_to_dataset(
        self,
        dataset_name: str,
        path: Path,
        patterns: Optional[List[str]] = None
    ) -> bool:
        """
        Upload a file or directory to a dataset.

        Args:
            dataset_name: Name of the target dataset
            path: Path to file or directory to upload
            patterns: Optional file patterns to filter

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Uploading to dataset: {dataset_name}")
        logger.info(f"Source: {path}")
        logger.info(f"{'='*60}")

        # Step 1: Find the dataset
        node_id = self.find_dataset_node_id(dataset_name)
        if not node_id:
            logger.error(f"Dataset not found: {dataset_name}")
            return False

        # Step 2: Set active dataset
        if not self.set_active_dataset(node_id):
            return False

        # Step 3: Get files to upload
        files = self.get_files_to_upload(path, patterns)

        if not files:
            logger.warning(f"No files to upload")
            return True

        logger.info(f"Found {len(files)} file(s) to upload")

        if self.verbose:
            for f in files:
                logger.debug(f"  - {f}")

        # Step 4: Create manifest with first file
        first_file = files[0]
        base_path = path if path.is_dir() else path.parent

        try:
            relative_path = first_file.relative_to(base_path)
            target_path = str(relative_path.parent) if relative_path.parent != Path('.') else None
        except ValueError:
            target_path = None

        manifest_id = self.create_manifest(first_file, target_path)
        if manifest_id is None:
            return False

        # Step 5: Add remaining files to manifest
        if len(files) > 1:
            logger.info(f"Adding {len(files) - 1} additional file(s) to manifest...")
            for file_path in files[1:]:
                if not self.add_to_manifest(manifest_id, file_path, base_path):
                    logger.error(f"Failed to add file: {file_path}")
                    return False

        # Step 6: Upload
        if not self.dry_run:
            logger.info("NOTE: Pennsieve does not overwrite files. Duplicates may be created.")

        if not self.upload_manifest(manifest_id):
            return False

        logger.info(f"Successfully uploaded to {dataset_name}")
        return True


def main():
    parser = argparse.ArgumentParser(
        description='Upload files and directories to Pennsieve datasets using the CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload a file or folder to a single dataset
  %(prog)s --datasets "My Dataset" --path /path/to/file_or_folder

  # Upload to multiple datasets
  %(prog)s --datasets "Dataset 1" "Dataset 2" --path /path/to/data

  # Upload with file pattern filter
  %(prog)s --datasets "My Dataset" --path /path/to/data --pattern .tsv .json

  # Named upload: match folder names to dataset names
  %(prog)s --source-dir /path/to/output --match-names

  # Named upload: specific datasets only
  %(prog)s --source-dir /path/to/output --match-names --datasets "PennEPI00001" "PennEPI00002"

  # Dry run
  %(prog)s --datasets "My Dataset" --path /path/to/data --dry-run
        """
    )

    # Simple upload mode
    parser.add_argument('--path', type=Path,
                        help='Path to file or directory to upload')
    parser.add_argument('--datasets', nargs='+',
                        help='Dataset name(s) to upload to')

    # Named upload mode
    parser.add_argument('--source-dir', type=Path,
                        help='Source directory containing dataset-named folders')
    parser.add_argument('--match-names', action='store_true',
                        help='Match folder names to dataset names (use with --source-dir)')

    # Common options
    parser.add_argument('--pattern', nargs='+', default=None,
                        help='Only upload files containing these patterns in their name')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without making changes')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    # Validate arguments
    simple_mode = args.path is not None
    named_mode = args.source_dir is not None and args.match_names

    if simple_mode and named_mode:
        parser.error("Cannot use --path with --source-dir --match-names")

    if not simple_mode and not named_mode:
        parser.error("Must specify either --path or --source-dir with --match-names")

    if simple_mode and not args.datasets:
        parser.error("--datasets is required when using --path")

    if args.source_dir and not args.match_names:
        parser.error("--source-dir requires --match-names flag")

    uploader = PennsieveUploader(dry_run=args.dry_run, verbose=args.verbose)

    if args.dry_run:
        logger.info("\n" + "="*60)
        logger.info("DRY RUN MODE - No actual uploads will occur")
        logger.info("="*60 + "\n")

    # Simple upload mode: upload path to dataset(s)
    if simple_mode:
        if not args.path.exists():
            logger.error(f"Path does not exist: {args.path}")
            sys.exit(1)

        success_count = 0
        failure_count = 0

        for dataset_name in args.datasets:
            try:
                if uploader.upload_to_dataset(dataset_name, args.path, args.pattern):
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logger.error(f"Error uploading to {dataset_name}: {e}")
                failure_count += 1

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Datasets: {len(args.datasets)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failure_count}")

        if failure_count > 0:
            sys.exit(1)

    # Named upload mode: match folder names to dataset names
    else:
        if not args.source_dir.exists():
            logger.error(f"Source directory does not exist: {args.source_dir}")
            sys.exit(1)

        if not args.source_dir.is_dir():
            logger.error(f"Source path is not a directory: {args.source_dir}")
            sys.exit(1)

        # Get dataset folders
        if args.datasets:
            # Specific datasets
            dataset_dirs = []
            for name in args.datasets:
                dir_path = args.source_dir / name
                if not dir_path.exists():
                    logger.error(f"Dataset folder does not exist: {dir_path}")
                    sys.exit(1)
                dataset_dirs.append((name, dir_path))
        else:
            # All folders in source-dir
            dataset_dirs = [
                (d.name, d) for d in args.source_dir.iterdir()
                if d.is_dir() and not d.name.startswith('.')
            ]

        if not dataset_dirs:
            logger.warning("No dataset folders found")
            sys.exit(0)

        logger.info(f"Found {len(dataset_dirs)} dataset folder(s)")

        success_count = 0
        failure_count = 0
        skipped_count = 0

        for dataset_name, dataset_dir in dataset_dirs:
            try:
                if uploader.upload_to_dataset(dataset_name, dataset_dir, args.pattern):
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logger.error(f"Error uploading {dataset_name}: {e}")
                failure_count += 1

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total datasets: {len(dataset_dirs)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failure_count}")

        if failure_count > 0:
            sys.exit(1)


if __name__ == '__main__':
    main()
