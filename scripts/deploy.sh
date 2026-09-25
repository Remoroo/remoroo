#!/bin/bash
set -e

# Remoroo CLI Deployment Script
# Purpose: Push main + tag v{version} to GitHub. Pushing the tag triggers Actions → Publish to PyPI automatically.
# Version: bumped automatically (patch + 1 past the latest tag), never typed. For a minor/major release,
# set a higher version in pyproject.toml first — a version above the latest tag is used as is.
# Before tagging: uv lock (refresh + commit if needed), then scripts/sync_and_build.sh — same checks as the publish workflow.

echo "🚀 Remoroo CLI Deployment Script"
echo "=================================="
echo ""

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "❌ Error: Not in a git repository"
    exit 1
fi

# Check if we're on the main branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "⚠️  Warning: You are on branch '$CURRENT_BRANCH', not 'main'"
    read -p "Do you want to switch to main? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🔄 Switching to main branch..."
        git checkout main
        git pull origin main
    else
        echo "❌ Deployment cancelled"
        exit 1
    fi
fi

# --- Refresh the vendored Remoroo Studio bundle BEFORE the commit step ---------
# This CLI repo ships the prebuilt studio in remoroo/_studio/ but can't build it
# (no studio source here). Build it from the monorepo sibling (or $REMOROO_STUDIO_DIR)
# so a plain `deploy.sh` always ships the latest studio; the refreshed bundle is
# then picked up by the "uncommitted changes" commit step below — no extra commands.
CLI_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STUDIO_DIR="${REMOROO_STUDIO_DIR:-$CLI_ROOT/../remoroo_studio}"
if [ -f "$STUDIO_DIR/scripts/bundle_studio.py" ]; then
    echo "🎨 Building + bundling Remoroo Studio from $STUDIO_DIR ..."
    if ! python3 "$STUDIO_DIR/scripts/bundle_studio.py"; then
        echo "❌ Studio bundle failed (need Node 22+ and npm to build it). Fix and re-run."
        exit 1
    fi
    echo ""
elif [ -f "$CLI_ROOT/remoroo/_studio/dist/index.html" ]; then
    echo "⚠️  Studio source not found at $STUDIO_DIR — shipping the already-committed bundle."
    echo "    (set REMOROO_STUDIO_DIR to rebuild it.)"
    echo ""
else
    echo "❌ No studio source ($STUDIO_DIR) and no committed bundle (remoroo/_studio/)."
    echo "    Set REMOROO_STUDIO_DIR to the studio checkout, then re-run."
    exit 1
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "📝 Uncommitted changes detected"
    git status --short
    echo ""
    read -p "Do you want to commit these changes? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "Enter commit message: " COMMIT_MSG
        if [ -z "$COMMIT_MSG" ]; then
            echo "❌ Error: Commit message cannot be empty"
            exit 1
        fi
        echo "📦 Staging all changes..."
        git add .
        echo "💾 Committing..."
        git commit -m "$COMMIT_MSG"
    else
        echo "❌ Deployment cancelled"
        exit 1
    fi
else
    echo "✅ Working directory is clean"
fi

# Confirm deployment
echo ""
echo "Ready to deploy to GitHub:"
echo "  Branch: main"
echo "  Remote: origin"
echo "  Commits to push: $(git log origin/main..HEAD --oneline | wc -l | xargs)"
echo ""

if [ "$(git log origin/main..HEAD --oneline | wc -l | xargs)" = "0" ]; then
    echo "ℹ️  No new commits to push."
fi

# Extract version from pyproject.toml
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: pyproject.toml not found"
    exit 1
fi

VERSION=$(grep "^version = " pyproject.toml | sed 's/version = "\(.*\)"/\1/')

if [ -z "$VERSION" ]; then
    echo "❌ Error: Could not extract version from pyproject.toml"
    exit 1
fi

# Auto-bump. A version that is already released (a tag at or above it exists) moves to
# the latest tag's patch + 1. A version above the latest tag was never released (a run
# that stopped before tagging, or a minor/major set by hand) and is used as is, so
# re-running never burns a number.
LATEST_TAG=$(git tag --sort=-v:refname | head -1)
if [ -n "$LATEST_TAG" ]; then
    echo "ℹ️  Latest existing tag: $LATEST_TAG"
    LATEST_VERSION="${LATEST_TAG#v}"
    HIGHER=$(printf '%s\n%s\n' "$VERSION" "$LATEST_VERSION" | sort -V | tail -1)
    if [ "$HIGHER" = "$LATEST_VERSION" ]; then
        if ! [[ "$LATEST_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "❌ Error: latest tag $LATEST_TAG is not vMAJOR.MINOR.PATCH — cannot bump past it"
            exit 1
        fi
        IFS='.' read -r major minor patch <<< "$LATEST_VERSION"
        NEXT_VERSION="$major.$minor.$((patch + 1))"
        echo "⬆️  Bumping version: $VERSION → $NEXT_VERSION"
        sed -i.bak "s/^version = \"[^\"]*\"/version = \"$NEXT_VERSION\"/" pyproject.toml
        rm -f pyproject.toml.bak
        VERSION="$NEXT_VERSION"
    fi
fi

TAG="v$VERSION"
echo "📦 Version: $VERSION"
echo "🏷️  Tag to create: $TAG"
echo ""

DEPLOY_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI_ROOT="$(cd "$DEPLOY_SCRIPT_DIR/.." && pwd)"

# Refresh lockfile so deploy does not fail on stale uv.lock (CI still enforces via uv lock --check on a clean checkout)
if ! command -v uv >/dev/null 2>&1; then
    echo "❌ uv is required: https://docs.astral.sh/uv/"
    exit 1
fi
echo "🔒 Refreshing uv.lock (the version bump changes it)..."
(cd "$CLI_ROOT" && uv lock)
if [ -n "$(git -C "$CLI_ROOT" status --porcelain pyproject.toml uv.lock)" ]; then
    echo "💾 Committing release files..."
    git -C "$CLI_ROOT" add pyproject.toml uv.lock
    git -C "$CLI_ROOT" commit -m "chore: release $TAG"
fi

echo "🔒 Verifying lockfile and building release artifacts (same as CI publish)..."
if ! bash "$DEPLOY_SCRIPT_DIR/sync_and_build.sh"; then
    echo "❌ Lock check or build failed."
    exit 1
fi
echo ""

read -p "Proceed with deployment of $TAG? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled"
    exit 1
fi

# Create tag
echo ""
echo "🏷️  Creating tag $TAG..."
if git tag -a "$TAG" -m "Release $VERSION"; then
    echo "✅ Tag $TAG created successfully"
else
    echo "❌ Error: Failed to create tag"
    exit 1
fi

# Push to GitHub
echo ""
echo "🔄 Pushing to origin/main..."
if git push origin main; then
    echo "✅ Successfully pushed main branch!"
else
    echo "❌ Error: Failed to push main branch"
    # Clean up tag if push failed
    git tag -d "$TAG" 2>/dev/null
    exit 1
fi

echo ""
echo "🔄 Pushing tag $TAG..."
if git push origin "$TAG"; then
    echo "✅ Successfully pushed tag $TAG!"
    echo ""
    echo "🎯 \"Publish to PyPI\" will run automatically on this tag (uv build + pip build check + PyPI upload)."
    echo "   Watch: https://github.com/$(git config --get remote.origin.url | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"
    echo ""
    echo "✨ Deployment complete!"
else
    echo "❌ Error: Failed to push tag"
    echo "   You may need to delete the remote tag: git push origin :refs/tags/$TAG"
    exit 1
fi
