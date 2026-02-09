#!/bin/bash
cd /opt/wdws
export PYTHONUNBUFFERED=1
/opt/wdws/venv/bin/python3 ingest_attachments.py --workers 10 >> /opt/wdws/attachment_ingest.log 2>&1
echo "EXIT CODE: $?" >> /opt/wdws/attachment_ingest.log
