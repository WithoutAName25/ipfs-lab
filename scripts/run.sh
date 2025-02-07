#!/bin/bash

set -e

log() {
  COLOR='\033[1;34m' # Blue
  NC='\033[0m'       # No color
  echo -e "${COLOR}${1}${NC}"
}

log "Waiting until containers have started..."
sleep 10

if [[ -n "$TOPOLOGY" ]]; then
  log "Creating $TOPOLOGY topology..."
  python3 -u topology.py -t "$TOPOLOGY"
else
  log "TOPOLOGY is not defined or empty. Skipping topology setup..."
fi

log "Waiting..."
sleep 5

log "Running simulation..."
python3 -u simulator.py

log "Waiting..."
sleep 5

log "Print topology..."
python3 -u topology.py -m

log "Finished - keeping container running..."
tail -f /dev/null
