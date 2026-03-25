#!/usr/bin/env bash
# 1) uv: lockfile check + uv build → dist/ (primary artifacts for PyPI upload in CI)
# 2) pip: python -m build → dist_pip_build/ (same PEP 517 project; fails if pip path breaks)
# Upload is done in GitHub Actions with pypa/gh-action-pypi-publish (not uv publish).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v uv >/dev/null 2>&1; then
  echo "❌ uv is required: https://docs.astral.sh/uv/"
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  PY=python3
else
  PY=python
fi

echo "🔒 uv lock --check"
uv lock --check

echo "📦 uv build → dist/"
rm -rf dist dist_pip_build
uv build

echo "📦 pip: $PY -m build → dist_pip_build/ (sanity check, same pyproject)"
"$PY" -m pip install -q --upgrade pip
"$PY" -m pip install -q "build>=1.0.0"
"$PY" -m build --outdir dist_pip_build

echo "✅ uv artifacts (used for PyPI in CI):"
ls -la dist/
echo "✅ pip build artifacts:"
ls -la dist_pip_build/
