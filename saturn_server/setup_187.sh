#!/usr/bin/env bash
# One-shot setup for the Athena BGE GPU server on 172.16.81.187.
# Idempotent: re-running is safe; it skips steps already done.
#
# Run as root on 187:
#   bash /tmp/setup_187.sh
#
# Stage 1 (this script): repos, kernel headers, nvidia driver, venv, deps.
# A reboot is required after the nvidia driver install — the script stops
# and prints the reboot command. Re-run this same script after reboot to
# continue (it will detect the driver loaded and skip ahead).

set -euo pipefail

LOG=/var/log/bge-server-setup.log
exec > >(tee -a "$LOG") 2>&1
echo "=== bge-server setup starting at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

OPT_DIR=/opt/bge-server
SOURCES=/etc/apt/sources.list
WDWS_IP=172.16.32.207

# ── Step 0: whitelist the wdws box in fail2ban (if installed) ────────
# Without this, fail2ban repeatedly bans the wdws box during normal
# admin use of this server, breaking any kind of bulk operation.
if command -v fail2ban-client >/dev/null 2>&1; then
    echo "[0/8] Whitelisting $WDWS_IP in fail2ban..."
    # Unban immediately if currently banned in any jail.
    fail2ban-client unban "$WDWS_IP" 2>/dev/null || true
    # Persist the exemption.
    install -d -m 0755 /etc/fail2ban
    cat > /etc/fail2ban/jail.d/99-wdws-whitelist.local <<EOF
[DEFAULT]
ignoreip = 127.0.0.1/8 ::1 $WDWS_IP
EOF
    systemctl reload fail2ban 2>/dev/null || systemctl restart fail2ban 2>/dev/null || true
    echo "  fail2ban: $WDWS_IP whitelisted"
else
    echo "[0/8] No fail2ban detected — skipping whitelist step"
fi

# ── Step 1: enable contrib + non-free apt components ─────────────────
# Drop a managed sources file rather than editing /etc/apt/sources.list,
# so we don't fight the installer-provided file or break it with a regex.
EXTRA_SOURCES=/etc/apt/sources.list.d/bge-contrib-nonfree.list
if [ ! -f "$EXTRA_SOURCES" ]; then
    echo "[1/8] Enabling contrib + non-free via $EXTRA_SOURCES..."
    cat > "$EXTRA_SOURCES" <<'EOF'
deb http://deb.debian.org/debian/ trixie contrib non-free
deb-src http://deb.debian.org/debian/ trixie contrib non-free
deb http://deb.debian.org/debian/ trixie-updates contrib non-free
deb-src http://deb.debian.org/debian/ trixie-updates contrib non-free
deb http://security.debian.org/debian-security trixie-security contrib non-free
deb-src http://security.debian.org/debian-security trixie-security contrib non-free
EOF
    apt-get update -qq
else
    echo "[1/8] contrib+non-free file already present — skipping"
fi

# ── Step 2: install build prerequisites ──────────────────────────────
echo "[2/8] Installing kernel headers + build tools..."
apt-get install -y \
    linux-headers-amd64 \
    build-essential \
    dkms \
    pkg-config \
    curl \
    ca-certificates

# ── Step 3: install nvidia driver (if not already loaded) ────────────
# Probe with nvidia-smi rather than lsmod: lsmod's '^nvidia ' regex misses
# this module on some kernels (the leading column can be exactly 'nvidia'
# followed by tabs not spaces, depending on column width). nvidia-smi is
# the actual capability we need anyway — if it works, the driver is good.
if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi --query-gpu=name --format=csv,noheader >/dev/null 2>&1; then
    echo "[3/8] nvidia driver already working — skipping install"
    REBOOT_NEEDED=0
else
    echo "[3/8] Installing nvidia-driver from non-free..."
    apt-get install -y nvidia-driver firmware-misc-nonfree
    REBOOT_NEEDED=1
fi

# ── Step 4: install python venv prerequisites ────────────────────────
echo "[4/8] Installing python3.13-venv + pip..."
apt-get install -y python3.13-venv python3-pip

# Stop if reboot needed before going on to GPU-dependent steps.
if [ "${REBOOT_NEEDED:-0}" = "1" ]; then
    echo ""
    echo "================================================================"
    echo "  Driver installed; REBOOT REQUIRED before continuing."
    echo "  After reboot, re-run this same script to finish the setup:"
    echo "    bash /tmp/setup_187.sh"
    echo "================================================================"
    echo "  Run:  systemctl reboot"
    exit 0
fi

# ── Step 5: verify GPUs visible ──────────────────────────────────────
echo "[5/8] Verifying nvidia-smi sees both T4s..."
nvidia-smi --query-gpu=index,name,memory.total,driver_version --format=csv

# ── Step 6: stage source and build venv ──────────────────────────────
if [ ! -d "$OPT_DIR" ]; then
    echo "ERROR: $OPT_DIR does not exist. scp the saturn_server tree to /opt/bge-server first." >&2
    exit 1
fi

echo "[6/8] Building Python venv..."
if [ ! -x "$OPT_DIR/venv/bin/python" ]; then
    python3.13 -m venv "$OPT_DIR/venv"
fi
"$OPT_DIR/venv/bin/pip" install --upgrade pip wheel >/dev/null

# T4 = compute 7.5; cu124 wheels are fine on driver 550+ (trixie ships 550.163).
# torch 2.6+ is required by current sentence-transformers (3.4+) — the older
# 2.5.x line refuses to load .bin checkpoints under safetensors-tightened code.
if ! "$OPT_DIR/venv/bin/python" -c 'import torch, sys; sys.exit(0 if torch.__version__.split("+")[0] >= "2.6" else 1)' 2>/dev/null; then
    echo "  Installing torch==2.6.0 cu124..."
    "$OPT_DIR/venv/bin/pip" install --upgrade --index-url https://download.pytorch.org/whl/cu124 \
        "torch==2.6.0"
fi
"$OPT_DIR/venv/bin/pip" install -r "$OPT_DIR/requirements.txt"

# ── Step 7: torch CUDA sanity ────────────────────────────────────────
echo "[7/8] Torch CUDA sanity check..."
"$OPT_DIR/venv/bin/python" - <<'PY'
import torch
print(f"torch={torch.__version__} cuda_avail={torch.cuda.is_available()} ngpu={torch.cuda.device_count()}")
for i in range(torch.cuda.device_count()):
    print(f"  GPU {i}: {torch.cuda.get_device_name(i)}")
PY

# ── Step 8: warm model cache ─────────────────────────────────────────
echo "[8/8] Pre-downloading model weights (BGE-M3 + reranker, ~5 GB)..."
"$OPT_DIR/venv/bin/python" - <<'PY'
from sentence_transformers import SentenceTransformer, CrossEncoder
SentenceTransformer("BAAI/bge-m3", device="cuda:0", trust_remote_code=True)
CrossEncoder("BAAI/bge-reranker-v2-m3",
             device=("cuda:1" if __import__("torch").cuda.device_count() > 1 else "cuda:0"),
             trust_remote_code=True)
print("models cached ok")
PY

echo ""
echo "=== Stage 1 + 2 done. Now configure /opt/bge-server/.env and start systemd. ==="
echo "    cp $OPT_DIR/.env.example $OPT_DIR/.env"
echo "    chmod 600 $OPT_DIR/.env"
echo "    \$EDITOR $OPT_DIR/.env   # set BGE_BEARER_TOKEN to match wdws HF_API_TOKEN"
echo "    cp $OPT_DIR/bge-embedding.service /etc/systemd/system/"
echo "    systemctl daemon-reload"
echo "    systemctl enable --now bge-embedding.service"
echo "    journalctl -u bge-embedding -f"
