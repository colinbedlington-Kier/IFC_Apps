#!/usr/bin/env bash
set -euo pipefail

echo "[cobieqc-bootstrap] Starting COBieQC runtime bootstrap"
python -m cobieqc_service.bootstrap
echo "[cobieqc-bootstrap] Bootstrap complete"
