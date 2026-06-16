#!/usr/bin/env bash
# 1) uv: lockfile check (frozen) + uv build → dist/ (primary artifacts for PyPI upload in CI)
# 2) pip: python -m build → dist_pip_build/ (same PEP 517 project; fails if pip path breaks)
# Upload is done in GitHub Actions with pypa/gh-action-pypi-publish (not uv publish).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v uv >/dev/null 2>&1; then
  echo "❌ uv is required: https://docs.astral.sh/uv/"
  exit 1
fi

echo "🔒 uv lock --check"
uv lock --check

echo "📦 uv build → dist/"
rm -rf dist dist_pip_build
uv build

echo "📦 pip: build → dist_pip_build/ (sanity check, same pyproject)"
# Run the PEP 517 'build' frontend in an ephemeral, isolated env via uvx so we
# don't install into a system/Homebrew Python (PEP 668 externally-managed).
# 'build' still creates its own isolated build env (pip path) for the wheel/sdist.
uvx --from "build>=1.0.0" pyproject-build --outdir dist_pip_build

echo "✅ uv artifacts (used for PyPI in CI):"
ls -la dist/
echo "✅ pip build artifacts:"
ls -la dist_pip_build/
