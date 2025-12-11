#!/usr/bin/env python3
"""
CLI for DatasetManager.

Provides command-line interface for all dataset management operations.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Set up import paths
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(1, str(_this_dir.parent.parent))

from shared.config import API_HOST
from shared.auth import PennsieveAuth

from .manager import DatasetManager

logger = logging.getLogger(__name__)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Pennsieve Dataset Manager - Update datasets with flexible options',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update dataset name and subtitle
  %(prog)s --api-key KEY --api-secret SECRET \\
      --datasets MyDataset \\
      --name "New Name" \\
      --subtitle "New description"

  # Update multiple datasets with same tags
  %(prog)s --api-key KEY --api-secret SECRET \\
      --datasets Dataset1 Dataset2 Dataset3 \\
      --tags epilepsy research human

  # Change owner for all datasets
  %(prog)s --api-key KEY --api-secret SECRET \\
      --all \\
      --owner "N:user:xxxx-xxxx-xxxx-xxxx"

  # Add team and update license
  %(prog)s --api-key KEY --api-secret SECRET \\
      --datasets MyDataset \\
      --add-team "N:team:xxxx-xxxx" \\
      --add-team-role manager \\
      --license "Creative Commons Attribution"

  # Add external reference (DOI)
  %(prog)s --api-key KEY --api-secret SECRET \\
      --datasets MyDataset \\
      --add-reference "10.1016/j.example.2025.01.001" \\
      --reference-type "IsDescribedBy"

  # Dry run to preview changes
  %(prog)s --api-key KEY --api-secret SECRET \\
      --all --dry-run \\
      --license "Creative Commons Attribution"

  # Clean up duplicate files (files with (1) suffix)
  %(prog)s --api-key KEY --api-secret SECRET \\
      --datasets PennEPI00086 PennEPI00087 \\
      --cleanup-duplicates participants.tsv "sub-{dataset}/sub-{dataset}_sessions.tsv"

Collaborator roles: viewer, editor, manager

Relationship types for references:
  IsDescribedBy, Describes, IsCitedBy, Cites, IsSupplementTo,
  IsReferencedBy, References, IsDerivedFrom, IsSourceOf, etc.
        """
    )

    # Authentication
    auth_group = parser.add_argument_group('Authentication')
    auth_group.add_argument('--api-key', required=True, help='Pennsieve API key')
    auth_group.add_argument('--api-secret', required=True, help='Pennsieve API secret')
    auth_group.add_argument('--api-host', default=API_HOST, help='API host URL')

    # Dataset selection
    select_group = parser.add_argument_group('Dataset Selection')
    select_group.add_argument('--datasets', nargs='+', help='Dataset name(s) to process')
    select_group.add_argument('--all', action='store_true', dest='all_datasets', help='Process all datasets')

    # Metadata options
    meta_group = parser.add_argument_group('Metadata')
    meta_group.add_argument('--name', help='New dataset name')
    meta_group.add_argument('--subtitle', help='Dataset subtitle/description')
    meta_group.add_argument('--tags', nargs='+', help='Set dataset tags (replaces existing)')
    meta_group.add_argument('--add-tags', nargs='+', help='Add tags (preserves existing)')
    meta_group.add_argument('--remove-tags', nargs='+', help='Remove specific tags')
    meta_group.add_argument('--license', dest='license_name', help='Dataset license')
    meta_group.add_argument('--readme', help='Dataset readme text')
    meta_group.add_argument('--banner', help='Path to banner image (PNG, JPG, GIF)')

    # Owner & collaborators
    collab_group = parser.add_argument_group('Owner & Collaborators')
    collab_group.add_argument('--owner', help='New owner user ID (N:user:xxxx)')
    collab_group.add_argument('--add-team', help='Team ID to add (N:team:xxxx)')
    collab_group.add_argument('--add-team-role', default='viewer', choices=['viewer', 'editor', 'manager'], help='Role for added team')
    collab_group.add_argument('--remove-team', help='Team ID to remove')
    collab_group.add_argument('--add-user', help='User ID to add (N:user:xxxx)')
    collab_group.add_argument('--add-user-role', default='viewer', choices=['viewer', 'editor', 'manager'], help='Role for added user')
    collab_group.add_argument('--remove-user', help='User ID to remove')

    # Contributors
    contrib_group = parser.add_argument_group('Contributors')
    contrib_group.add_argument('--contributors', nargs='+', type=int, help='Contributor IDs to add')
    contrib_group.add_argument('--remove-contributors', nargs='+', type=int, help='Contributor IDs to remove')

    # References
    ref_group = parser.add_argument_group('References (External Publications)')
    ref_group.add_argument('--add-reference', help='DOI to add')
    ref_group.add_argument('--remove-reference', help='DOI to remove')
    ref_group.add_argument('--reference-type', default='IsDescribedBy', help='Relationship type for reference')

    # Delete operations
    delete_group = parser.add_argument_group('Delete Operations')
    delete_group.add_argument(
        '--delete-pattern', metavar='PATTERN',
        help='Delete files matching glob pattern (e.g., "*.tsv", "*_ieeg.json")'
    )
    delete_group.add_argument(
        '--delete-path', nargs='+', metavar='PATH',
        help='Delete specific files by path (e.g., "ieeg/file.tsv" "README.txt")'
    )
    delete_group.add_argument(
        '--cleanup-duplicates', nargs='+', metavar='FILE',
        help='File paths to check for (1) duplicates. Use {dataset} as placeholder.'
    )

    # Execution options
    exec_group = parser.add_argument_group('Execution Options')
    exec_group.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    exec_group.add_argument('--force-reload', action='store_true', help='Bypass dataset cache')
    exec_group.add_argument('--verbose', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Validate dataset selection
    if not args.datasets and not args.all_datasets:
        parser.error("Must provide either --datasets or --all")

    if args.datasets and args.all_datasets:
        parser.error("Cannot use both --datasets and --all")

    # Check if any update options provided
    update_options = [
        args.name, args.subtitle, args.tags, args.add_tags, args.remove_tags,
        args.license_name, args.readme, args.banner, args.owner,
        args.add_team, args.remove_team, args.add_user, args.remove_user,
        args.contributors, args.remove_contributors,
        args.add_reference, args.remove_reference,
        args.delete_pattern, args.delete_path, args.cleanup_duplicates
    ]
    if not any(update_options):
        logger.warning("No update options provided - will authenticate but take no actions")

    # Display mode
    if args.dry_run:
        logger.info("\n" + "="*60)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("="*60)

    # Authenticate
    auth = PennsieveAuth(args.api_host)
    try:
        auth.authenticate(args.api_key, args.api_secret)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)

    # Initialize manager
    manager = DatasetManager(auth, args.api_host, dry_run=args.dry_run)

    # Get dataset names
    if args.all_datasets:
        logger.info("Fetching all datasets...")
        all_datasets = manager.fetch_all_datasets(force_reload=args.force_reload)
        dataset_names = [
            ds.get("content", {}).get("name")
            for ds in all_datasets
            if ds.get("content", {}).get("name")
        ]
        logger.info(f"Found {len(dataset_names)} datasets")
    else:
        dataset_names = args.datasets

    # Process datasets
    total = len(dataset_names)
    succeeded = 0
    failed = 0
    delete_success = 0
    delete_failed = 0
    cleanup_success = 0
    cleanup_skipped = 0

    for dataset_name in dataset_names:
        # Standard metadata updates
        if manager.process_dataset(
            dataset_name,
            name=args.name,
            subtitle=args.subtitle,
            tags=args.tags,
            add_tags=args.add_tags,
            remove_tags=args.remove_tags,
            license_name=args.license_name,
            readme=args.readme,
            banner=args.banner,
            owner=args.owner,
            add_team=args.add_team,
            add_team_role=args.add_team_role,
            remove_team=args.remove_team,
            add_user=args.add_user,
            add_user_role=args.add_user_role,
            remove_user=args.remove_user,
            contributors=args.contributors,
            remove_contributors=args.remove_contributors,
            add_reference=args.add_reference,
            reference_type=args.reference_type,
            remove_reference=args.remove_reference
        ):
            succeeded += 1
        else:
            failed += 1

        # Delete by pattern if requested
        if args.delete_pattern:
            success, fail = manager.delete_by_pattern(
                dataset_name,
                args.delete_pattern,
                force_reload=args.force_reload
            )
            delete_success += success
            delete_failed += fail

        # Delete by path if requested
        if args.delete_path:
            success, fail = manager.delete_by_path(
                dataset_name,
                args.delete_path,
                force_reload=args.force_reload
            )
            delete_success += success
            delete_failed += fail

        # Cleanup duplicates if requested
        if args.cleanup_duplicates:
            success, skipped = manager.cleanup_duplicates(
                dataset_name,
                args.cleanup_duplicates,
                force_reload=args.force_reload
            )
            cleanup_success += success
            cleanup_skipped += skipped

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total datasets: {total}")
    logger.info(f"Succeeded: {succeeded}")
    logger.info(f"Failed: {failed}")
    if args.delete_pattern or args.delete_path:
        logger.info(f"Files deleted: {delete_success}")
        logger.info(f"Delete failures: {delete_failed}")
    if args.cleanup_duplicates:
        logger.info(f"Duplicates cleaned: {cleanup_success}")
        logger.info(f"Duplicates skipped: {cleanup_skipped}")

    if args.dry_run:
        logger.info("\n(Dry-run mode - no changes were made)")

    if failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
