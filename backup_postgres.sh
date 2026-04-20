#!/bin/bash
# Athena Cognitive Engine — PostgreSQL Backup
# Runs daily via cron. Keeps 14 days of backups.

BACKUP_DIR="/opt/wdws/backups"
RETENTION_DAYS=14
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"

# Backup wdws database
echo "[$(date)] Backing up wdws..."
sudo -u postgres pg_dump -Fc wdws > "$BACKUP_DIR/wdws_${DATE}.dump" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "[$(date)] wdws backup OK: $(du -h "$BACKUP_DIR/wdws_${DATE}.dump" | cut -f1)"
else
    echo "[$(date)] ERROR: wdws backup failed"
fi

# Backup athena_chat database
echo "[$(date)] Backing up athena_chat..."
sudo -u postgres pg_dump -Fc athena_chat > "$BACKUP_DIR/athena_chat_${DATE}.dump" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "[$(date)] athena_chat backup OK: $(du -h "$BACKUP_DIR/athena_chat_${DATE}.dump" | cut -f1)"
else
    echo "[$(date)] ERROR: athena_chat backup failed"
fi

# Prune old backups
echo "[$(date)] Pruning backups older than ${RETENTION_DAYS} days..."
find "$BACKUP_DIR" -name "*.dump" -mtime +${RETENTION_DAYS} -delete 2>/dev/null
echo "[$(date)] Backup complete. Files:"
ls -lh "$BACKUP_DIR"/*.dump 2>/dev/null | tail -10
