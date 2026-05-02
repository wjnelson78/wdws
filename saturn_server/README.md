# Athena BGE GPU Server — sister deployment

Drop-in TEI-compatible embedder + reranker. Built to run alongside (and act
as a hot standby for) the saturn box at `172.16.81.113:9098`. The first
deployment of this code targets `172.16.81.187` (2× Tesla T4).

## Why a sister box

Saturn's `embedding_service.py` lives only on saturn — undocumented in git,
no recovery path if the VM is lost. It also flaps (warm-probe at 19:14 UTC
worked, eight minutes later it TCP-timed-out). Goals:

1. **Tracked source** — code lives in `/opt/wdws/saturn_server/` so it can
   be redeployed cleanly.
2. **HA via two endpoints** — `embedding_service.py` now reads
   `HF_ENDPOINT_URL_FALLBACK` and tries it per-batch when the primary fails,
   before dropping to CPU.
3. **More throughput** — 187 has 2 T4s; embedder pins to GPU 0, reranker to
   GPU 1. Saturn loaded both onto one T4.

## Wire model — TEI-compatible

Same routes as TEI / saturn, so the wdws client doesn't change:

- `POST /embed`   `{"inputs": [str, ...]}`               → `[[float, ...], ...]`
- `POST /rerank`  `{"query": str, "texts": [str, ...]}`  → `[{"index": int, "score": float}, ...]`
- `GET  /health`                                         → `"ok"`
- `GET  /info`                                           → diagnostics

Bearer auth: `Authorization: Bearer <BGE_BEARER_TOKEN>`. Token must equal
the wdws client's `HF_API_TOKEN`.

## Deployment to 172.16.81.187

These steps run **on 187**, as `root`. Files come from the wdws box
(`/opt/wdws/saturn_server/`).

### 1. Stage source on 187

From the wdws box (172.16.32.207):

```bash
# Replace <user> with whatever account has SSH access on 187.
scp -r /opt/wdws/saturn_server/ <user>@172.16.81.187:/tmp/saturn_server
ssh <user>@172.16.81.187 "sudo mv /tmp/saturn_server /opt/bge-server && sudo chown -R root:root /opt/bge-server"
```

### 2. Verify GPUs

```bash
nvidia-smi
# Expect: 2× Tesla T4, driver loaded.
```

### 3. Build venv with CUDA-matched torch

T4 = compute capability 7.5 → CUDA 11.8 or 12.1 wheels both work. Pick
whatever matches the host driver (`nvidia-smi` top-right). The example
below uses cu121:

```bash
cd /opt/bge-server
python3 -m venv venv
./venv/bin/pip install --upgrade pip wheel
./venv/bin/pip install --index-url https://download.pytorch.org/whl/cu121 \
    torch==2.3.1
./venv/bin/pip install -r requirements.txt
```

### 4. Configure env

```bash
cp /opt/bge-server/.env.example /opt/bge-server/.env
chmod 600 /opt/bge-server/.env
# Edit /opt/bge-server/.env:
#   BGE_BEARER_TOKEN = exact value of HF_API_TOKEN from /opt/wdws/.env on the wdws box
```

### 5. First-time model warm — pull weights once

The first `import sentence_transformers` will download BGE-M3 (~2.3 GB) and
the reranker (~2.2 GB) into `~/.cache/huggingface`. Do this before enabling
the unit so systemd's start timeout doesn't trip:

```bash
cd /opt/bge-server
./venv/bin/python -c "
from sentence_transformers import SentenceTransformer, CrossEncoder
SentenceTransformer('BAAI/bge-m3', device='cuda:0', trust_remote_code=True)
CrossEncoder('BAAI/bge-reranker-v2-m3', device='cuda:1', trust_remote_code=True)
print('ok')
"
```

### 6. Smoke test the server in the foreground

```bash
cd /opt/bge-server
set -a; . ./.env; set +a
./venv/bin/python ./gpu_embedding_server.py
```

In a second shell on 187:

```bash
TOKEN=$(grep BGE_BEARER_TOKEN /opt/bge-server/.env | cut -d= -f2)
curl -s http://127.0.0.1:9098/health
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9098/info | jq .
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  http://127.0.0.1:9098/embed \
  -d '{"inputs":["hello world","second text"]}' | jq 'length, .[0] | length'
# Expect: 2 then 1024
```

`Ctrl-C` the server.

### 7. Install systemd unit

```bash
sudo cp /opt/bge-server/bge-embedding.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bge-embedding.service
sudo systemctl status bge-embedding.service
journalctl -u bge-embedding.service -f -n 100
```

Wait for `Server ready: embed=cuda:0 rerank=cuda:1 concurrency=4` in the log.

### 8. Smoke test from the wdws box

From 172.16.32.207:

```bash
TOKEN=$(grep ^HF_API_TOKEN= /opt/wdws/.env | cut -d= -f2)
curl -s --max-time 5 http://172.16.81.187:9098/health
curl -s --max-time 10 -H "Authorization: Bearer $TOKEN" \
  http://172.16.81.187:9098/info | jq .
curl -s --max-time 30 -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  http://172.16.81.187:9098/embed \
  -d '{"inputs":["test"]}' | jq '.[0] | length'
# Expect: 1024
```

### 9. Wire the wdws client to fail over

Edit `/opt/wdws/.env`. Keep saturn as primary for now (it has been
warm-cached longer); add 187 as the standby:

```
HF_ENDPOINT_URL=http://172.16.81.113:9098
HF_ENDPOINT_URL_FALLBACK=http://172.16.81.187:9098
HF_RERANKER_ENDPOINT_URL=http://172.16.81.113:9098
HF_RERANKER_ENDPOINT_URL_FALLBACK=http://172.16.81.187:9098
```

Then bounce any long-running wdws process so it picks the new env up
(`embedding_service.HF_FALLBACK_ENABLED` resolves at module load).

To **promote 187 to primary** once it has soaked: swap the two URLs.

## Operational notes

- **Concurrency**: `BGE_CONCURRENCY=4` is the saturn-tested ceiling. T4
  peaks at ~8 GB VRAM under that load with embedder fp16 + reranker fp16,
  with plenty of headroom.
- **Cold start**: 30–60 s to load both models. `TimeoutStartSec=300` in the
  unit covers it.
- **Offline mode**: after the first download, set `HF_HUB_OFFLINE=1` and
  `TRANSFORMERS_OFFLINE=1` in `.env` to skip every HF.co round-trip.
- **VRAM stability**: `torch.cuda.empty_cache()` runs in every `finally`
  block so long uptime doesn't accumulate tensors.
- **Reranker normalization**: `raw_scores=false` (default) returns sigmoid-
  normalized scores in `[0,1]`, matching TEI. The wdws client sends
  `raw_scores: False` already.

## Rollback

```bash
sudo systemctl disable --now bge-embedding.service
# Edit /opt/wdws/.env on the wdws box: drop HF_ENDPOINT_URL_FALLBACK,
# revert HF_ENDPOINT_URL to saturn-only.
```
