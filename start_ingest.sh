#!/bin/bash
# Start the WDWS ingest pipeline with logging
cd /opt/wdws
source venv/bin/activate
nohup python ingest.py --domain all > ingest_full.log 2>&1 &
echo "PID: $!"
echo "Monitor with: ssh root@172.16.32.207 tail -f /opt/wdws/ingest_full.log"
