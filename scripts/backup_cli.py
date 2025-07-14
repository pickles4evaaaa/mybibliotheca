#!/usr/bin/env python3
"""
Bibliotheca Backup CLI Tool

Command-line interface for backup and restore operations.
Can be used for automated scripts, cron jobs, or manual operations.
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
import logging

# Add the parent directory to the path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.backup_restore_service import BackupRestoreService, BackupType


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_backup(args):
    """Create a new backup."""
    try:
        service = BackupRestoreService(args.base_dir)
        
        # Convert backup type string to enum
        backup_type = BackupType(args.type.lower())
        
        print(f"Creating {backup_type.value} backup...")
        
        backup_info = service.create_backup(
            backup_type=backup_type,
            name=args.name,
            description=args.description or f"CLI backup created on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        if backup_info:
            print(f"✅ Backup created successfully!")
            print(f"   ID: {backup_info.id}")
            print(f"   Name: {backup_info.name}")
            print(f"   Size: {backup_info.file_size / (1024 * 1024):.2f} MB")
            print(f"   Location: {backup_info.file_path}")
            return True
        else:
            print("❌ Failed to create backup")
            return False
            
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return False


def list_backups(args):
    """List all available backups."""
    try:
        service = BackupRestoreService(args.base_dir)
        backups = service.list_backups()
        
        if not backups:
            print("No backups found.")
            return True
        
        print(f"Found {len(backups)} backups:")
        print()
        
        # Sort by creation date (newest first)
        backups.sort(key=lambda b: b.created_at, reverse=True)
        
        for backup in backups:
            status_icon = "✅" if backup.status.value == "completed" else "❌"
            size_mb = backup.file_size / (1024 * 1024)
            
            print(f"{status_icon} {backup.name}")
            print(f"   ID: {backup.id}")
            print(f"   Type: {backup.backup_type.value}")
            print(f"   Created: {backup.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Size: {size_mb:.2f} MB")
            print(f"   Status: {backup.status.value}")
            if backup.description:
                print(f"   Description: {backup.description}")
            print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error listing backups: {e}")
        return False


def restore_backup(args):
    """Restore from a backup."""
    try:
        service = BackupRestoreService(args.base_dir)
        
        # Get backup info
        backup_info = service.get_backup(args.backup_id)
        if not backup_info:
            print(f"❌ Backup not found: {args.backup_id}")
            return False
        
        print(f"Restoring from backup: {backup_info.name}")
        print(f"Created: {backup_info.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Type: {backup_info.backup_type.value}")
        print()
        
        if not args.yes:
            confirm = input("This will overwrite current data. Continue? (y/N): ")
            if confirm.lower() != 'y':
                print("Restore cancelled.")
                return True
        
        print("Starting restore...")
        success = service.restore_backup(args.backup_id, args.restore_path)
        
        if success:
            print("✅ Restore completed successfully!")
            if args.restore_path:
                print(f"   Restored to: {args.restore_path}")
            else:
                print("   Restored to current location")
            return True
        else:
            print("❌ Restore failed")
            return False
            
    except Exception as e:
        print(f"❌ Error restoring backup: {e}")
        return False


def delete_backup(args):
    """Delete a backup."""
    try:
        service = BackupRestoreService(args.base_dir)
        
        backup_info = service.get_backup(args.backup_id)
        if not backup_info:
            print(f"❌ Backup not found: {args.backup_id}")
            return False
        
        print(f"Deleting backup: {backup_info.name}")
        print(f"Created: {backup_info.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Size: {backup_info.file_size / (1024 * 1024):.2f} MB")
        print()
        
        if not args.yes:
            confirm = input("Are you sure you want to delete this backup? (y/N): ")
            if confirm.lower() != 'y':
                print("Delete cancelled.")
                return True
        
        success = service.delete_backup(args.backup_id)
        
        if success:
            print("✅ Backup deleted successfully!")
            return True
        else:
            print("❌ Failed to delete backup")
            return False
            
    except Exception as e:
        print(f"❌ Error deleting backup: {e}")
        return False


def cleanup_backups(args):
    """Clean up old backups."""
    try:
        service = BackupRestoreService(args.base_dir)
        
        print(f"Cleaning up backups older than {args.max_age} days or exceeding {args.max_count} total...")
        
        deleted_count = service.cleanup_old_backups(
            max_age_days=args.max_age,
            max_count=args.max_count
        )
        
        if deleted_count > 0:
            print(f"✅ Cleaned up {deleted_count} old backups")
        else:
            print("ℹ️  No backups needed cleanup")
        
        return True
        
    except Exception as e:
        print(f"❌ Error cleaning up backups: {e}")
        return False


def export_data(args):
    """Export data to file."""
    try:
        service = BackupRestoreService(args.base_dir)
        
        print(f"Exporting data in {args.format} format...")
        
        export_path = service.export_data(args.format)
        
        if export_path:
            print(f"✅ Data exported successfully!")
            print(f"   Location: {export_path}")
            return True
        else:
            print("❌ Failed to export data")
            return False
            
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        return False


def show_stats(args):
    """Show backup statistics."""
    try:
        from app.services.backup_restore_service import get_backup_stats
        stats = get_backup_stats()
        
        print("Backup Statistics:")
        print(f"  Total Backups: {stats['total_backups']}")
        print(f"  Total Size: {stats['total_size_mb']} MB")
        
        if stats['oldest_backup']:
            print(f"  Oldest Backup: {stats['oldest_backup']}")
        if stats['newest_backup']:
            print(f"  Newest Backup: {stats['newest_backup']}")
        
        if stats['backup_types']:
            print("  Backup Types:")
            for backup_type, count in stats['backup_types'].items():
                print(f"    {backup_type}: {count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error getting stats: {e}")
        return False


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Bibliotheca Backup CLI Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s create --type full --name "weekly-backup"
  %(prog)s list
  %(prog)s restore abc123def456
  %(prog)s cleanup --max-age 30 --max-count 50
  %(prog)s export --format csv
        """
    )
    
    parser.add_argument('--base-dir', default='.',
                       help='Base directory for the application (default: current directory)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create backup command
    create_parser = subparsers.add_parser('create', help='Create a new backup')
    create_parser.add_argument('--type', choices=['full', 'data_only', 'kuzu_only', 'config_only'],
                              default='full', help='Type of backup to create')
    create_parser.add_argument('--name', help='Custom name for the backup')
    create_parser.add_argument('--description', help='Description for the backup')
    
    # List backups command
    list_parser = subparsers.add_parser('list', help='List all backups')
    
    # Restore backup command
    restore_parser = subparsers.add_parser('restore', help='Restore from a backup')
    restore_parser.add_argument('backup_id', help='ID of the backup to restore')
    restore_parser.add_argument('--restore-path', help='Path to restore to (default: current location)')
    restore_parser.add_argument('--yes', '-y', action='store_true',
                               help='Skip confirmation prompt')
    
    # Delete backup command
    delete_parser = subparsers.add_parser('delete', help='Delete a backup')
    delete_parser.add_argument('backup_id', help='ID of the backup to delete')
    delete_parser.add_argument('--yes', '-y', action='store_true',
                              help='Skip confirmation prompt')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old backups')
    cleanup_parser.add_argument('--max-age', type=int, default=30,
                               help='Maximum age in days (default: 30)')
    cleanup_parser.add_argument('--max-count', type=int, default=50,
                               help='Maximum number of backups (default: 50)')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to file')
    export_parser.add_argument('--format', choices=['csv', 'json'], default='csv',
                              help='Export format (default: csv)')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show backup statistics')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    setup_logging(args.verbose)
    
    # Command dispatch
    commands = {
        'create': create_backup,
        'list': list_backups,
        'restore': restore_backup,
        'delete': delete_backup,
        'cleanup': cleanup_backups,
        'export': export_data,
        'stats': show_stats
    }
    
    try:
        success = commands[args.command](args)
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
