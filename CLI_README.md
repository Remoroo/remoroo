# Remoroo CLI (developer note)

End-user documentation lives in **[README.md](./README.md)** (installation, v2 behavior, artifact layout, flags, troubleshooting).

## Install from this repo

```bash
pip install .
```

## Quick usage

```bash
remoroo run --local --goal "..." --metrics "..."
```

**v2** is the only supported agent loop; legacy v1 is not available. Local runs use the default hosted Brain out of the box; auth via `remoroo login` or `REMOROO_API_KEY`. Override the Brain URL only for self-hosted setups (`REMOROO_API_URL`).

Run outputs: **`<repo>/.remoroo/runs/<run-id>/`** (reports, patches, trace, checkpoints). See the main README for the full flag list and architecture.
