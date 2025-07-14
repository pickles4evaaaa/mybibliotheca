# Backup & Restore System Documentation

The Bibliotheca backup and restore system provides comprehensive data protection and recovery capabilities for your library management system.

## Features

### 🛡️ Backup Types

- **Full Backup**: Complete system backup including data, database, and configuration files
- **Data Only**: User data and uploaded files only  
- **Database Only**: KuzuDB database files only
- **Configuration Only**: Application configuration files only

### 📅 Scheduling

- Automated daily, weekly, or monthly backups
- Configurable backup retention policies
- Background execution with notification support
- Integration with systemd timers (Linux) or cron jobs

### 🔧 Management

- Web-based administration interface
- Command-line tools for automation
- Backup verification and integrity checking
- One-click restore functionality

### 📊 Monitoring

- Backup status dashboard
- Storage usage statistics
- Backup history and logs
- Email notifications (configurable)

## Getting Started

### Access the Backup System

1. Log in as an administrator
2. Go to **Admin Dashboard**
3. Click **Manage Backups** in the Backup & Maintenance section

### Create Your First Backup

1. Click **Create Backup** button
2. Select backup type (Full Backup recommended)
3. Add optional name and description
4. Click **Create Backup**

The backup will be created in the background and saved to the `data/backups/` directory.

## Web Interface

### Main Backup Page (`/admin/backup/`)

- **Backup Statistics**: Overview of total backups, storage usage, and recent activity
- **Backup List**: All available backups with download/restore/delete options
- **Quick Actions**: Create new backups and access other tools

### Export Data (`/admin/backup/export`)

- **CSV Export**: Library data in spreadsheet format
- **JSON Export**: Complete structured data export
- **Compatibility**: Works with Excel, Google Sheets, and analysis tools

### Schedule Management (`/admin/backup/schedule`)

- **Frequency Settings**: Daily, weekly, or monthly backups
- **Retention Policies**: Automatic cleanup of old backups
- **Advanced Options**: Compression, verification, and triggers

## Command Line Interface

The backup system includes a powerful CLI tool for automation and scripting.

### Installation

The CLI script is located at `scripts/backup_cli.py` and can be used directly:

```bash
# Make executable (Linux/macOS)
chmod +x scripts/backup_cli.py

# Or run with Python
python3 scripts/backup_cli.py --help
```

### Basic Commands

```bash
# Create a full backup
./scripts/backup_cli.py create --type full --name "my-backup"

# List all backups
./scripts/backup_cli.py list

# Restore from backup
./scripts/backup_cli.py restore backup-id-here

# Clean up old backups
./scripts/backup_cli.py cleanup --max-age 30 --max-count 50

# Export data
./scripts/backup_cli.py export --format csv

# Show statistics
./scripts/backup_cli.py stats
```

### Advanced Usage

```bash
# Create backup with description
./scripts/backup_cli.py create --type full --name "pre-update-backup" --description "Backup before system update"

# Restore to specific location
./scripts/backup_cli.py restore backup-id --restore-path /path/to/restore/location

# Force operations without confirmation
./scripts/backup_cli.py delete backup-id --yes
./scripts/backup_cli.py restore backup-id --yes

# Verbose output
./scripts/backup_cli.py --verbose create --type full
```

## Automation & Scheduling

### Linux (systemd)

Use the provided systemd configuration:

```bash
# Copy service files
sudo cp scripts/systemd-backup-config.txt /etc/systemd/system/bibliotheca-backup.service
sudo cp scripts/systemd-backup-config.txt /etc/systemd/system/bibliotheca-backup.timer

# Edit paths and user/group as needed
sudo nano /etc/systemd/system/bibliotheca-backup.service

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable bibliotheca-backup.timer
sudo systemctl start bibliotheca-backup.timer

# Check status
sudo systemctl status bibliotheca-backup.timer
```

### Docker Compose

Add a backup scheduler service to your `docker-compose.yml`:

```yaml
services:
  backup-scheduler:
    image: alpine:latest
    volumes:
      - ./:/app
      - ./backups:/app/backups
    working_dir: /app
    command: >
      sh -c "
      apk add --no-cache python3 py3-pip &&
      pip3 install -r requirements.txt &&
      while true; do
        python3 scripts/backup_cli.py create --type full
        sleep 86400  # 24 hours
      done"
    restart: unless-stopped
    depends_on:
      - bibliotheca
```

### Manual Cron Jobs

```bash
# Edit crontab
crontab -e

# Add daily backup at 2 AM
0 2 * * * cd /path/to/bibliotheca && python3 scripts/backup_cli.py create --type full >/dev/null 2>&1

# Add weekly cleanup
0 3 * * 0 cd /path/to/bibliotheca && python3 scripts/backup_cli.py cleanup --max-age 30 --max-count 50 >/dev/null 2>&1
```

## Backup Storage

### File Structure

```
backups/
├── backup_index.json                 # Backup metadata index
├── full_20240714_143022.tar.gz      # Full backup archive
├── data_20240713_020000.tar.gz      # Data-only backup
└── kuzu_20240712_120000.tar.gz      # Database-only backup
```

### Archive Contents

**Full Backup** includes:
- `data/` - User data and uploaded files (including KuzuDB at `data/kuzu/`)
- `config.py` - Application configuration
- `.env` - Environment variables
- `docker-compose.yml` - Docker configuration
- `requirements.txt` - Python dependencies
- `backup_metadata.json` - Backup information

### Storage Considerations

- Backup files are compressed using gzip
- Full backups can be large (depends on your library size)
- Plan for 2-3x your current data size for backup storage
- Consider external storage for production systems

## Restore Process

### Web Interface Restore

1. Go to **Admin → Backup & Restore**
2. Find the backup you want to restore
3. Click the **Restore** button (🔄)
4. Type "yes" to confirm
5. Click **Restore Backup**

**Important**: The system automatically creates a backup of the current state before restoring.

### CLI Restore

```bash
# List available backups
./scripts/backup_cli.py list

# Restore specific backup
./scripts/backup_cli.py restore abc123def456

# Restore to different location
./scripts/backup_cli.py restore abc123def456 --restore-path /path/to/restore
```

### Docker Restore

For Docker deployments:

1. Stop the application: `docker-compose down`
2. Extract backup to the appropriate location
3. Start the application: `docker-compose up -d`

## Best Practices

### 🔄 Regular Backups

- **Development**: Daily backups
- **Production**: Multiple daily backups with longer retention
- **Critical Systems**: Real-time or hourly backups

### 🔒 Security

- Store backups in secure locations
- Consider encryption for sensitive data
- Limit access to backup files
- Regular restore testing

### 📈 Monitoring

- Check backup completion notifications
- Monitor backup file sizes for anomalies  
- Test restore procedures regularly
- Document your backup strategy

### 🧹 Maintenance

- Regular cleanup of old backups
- Monitor storage usage
- Update backup scripts with system changes
- Keep backup documentation current

## Troubleshooting

### Common Issues

**Permission Errors**
```bash
# Fix file permissions
sudo chown -R bibliotheca:bibliotheca /path/to/bibliotheca/data/backups
chmod 755 /path/to/bibliotheca/data/backups
```

**Disk Space Issues**
```bash
# Check available space
df -h /path/to/bibliotheca

# Clean up old backups
./scripts/backup_cli.py cleanup --max-age 7 --max-count 10
```

**Backup Failures**
- Check application logs
- Verify file permissions
- Ensure sufficient disk space
- Check database connectivity

**Restore Issues**
- Verify backup file integrity
- Check file permissions
- Ensure target location is writable
- Stop application services before restore

### Log Files

Backup operations are logged to:
- Application logs (web interface)
- System journal (systemd services)
- Console output (CLI operations)

### Getting Help

1. Check this documentation
2. Review application logs
3. Test with CLI tools
4. Check GitHub issues
5. Contact system administrator

## API Reference

### Backup Service Methods

```python
from app.services.backup_restore_service import get_backup_service

service = get_backup_service()

# Create backup
backup_info = service.create_backup(BackupType.FULL, name="my-backup")

# List backups
backups = service.list_backups()

# Restore backup
success = service.restore_backup(backup_id)

# Export data
export_path = service.export_data("csv")

# Get statistics
stats = service.get_backup_stats()
```

### REST API Endpoints

- `GET /admin/backup/` - Backup management page
- `POST /admin/backup/create` - Create new backup
- `POST /admin/backup/restore/<id>` - Restore backup
- `GET /admin/backup/download/<id>` - Download backup file
- `DELETE /admin/backup/delete/<id>` - Delete backup
- `GET /admin/backup/api/stats` - Get backup statistics

## Configuration

### Environment Variables

```bash
# Backup directory (default: ./data/backups)
BIBLIOTHECA_BACKUP_DIR=/path/to/backups

# Maximum backup age in days (default: 30)  
BIBLIOTHECA_BACKUP_MAX_AGE=30

# Maximum number of backups (default: 50)
BIBLIOTHECA_BACKUP_MAX_COUNT=50

# Enable/disable scheduled backups (default: true)
BIBLIOTHECA_BACKUP_ENABLED=true

# Backup notification email
BIBLIOTHECA_BACKUP_EMAIL=admin@example.com
```

### Application Configuration

Edit `config.py` to customize backup behavior:

```python
class Config:
    # Backup settings
    BACKUP_DIRECTORY = os.environ.get('BIBLIOTHECA_BACKUP_DIR', './data/backups')
    BACKUP_MAX_AGE_DAYS = int(os.environ.get('BIBLIOTHECA_BACKUP_MAX_AGE', 30))
    BACKUP_MAX_COUNT = int(os.environ.get('BIBLIOTHECA_BACKUP_MAX_COUNT', 50))
    BACKUP_ENABLED = os.environ.get('BIBLIOTHECA_BACKUP_ENABLED', 'true').lower() == 'true'
```

---

This backup system provides enterprise-grade data protection for your Bibliotheca installation. Regular backups ensure your library data is safe and recoverable in any situation.
