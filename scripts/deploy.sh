#!/bin/bash
set -e

# Remoroo CLI Deployment Script
# Purpose: Deploy changes to GitHub main branch and trigger CI workflows
# Note: This script does NOT create or modify tags

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

TAG="v$VERSION"
echo "📦 Version from pyproject.toml: $VERSION"
echo "🏷️  Tag to create: $TAG"
echo ""

# Check latest existing tag
LATEST_TAG=$(git tag --sort=-v:refname | head -1)
if [ -n "$LATEST_TAG" ]; then
    echo "ℹ️  Latest existing tag: $LATEST_TAG"
    
    # Extract version from latest tag (remove 'v' prefix)
    LATEST_VERSION="${LATEST_TAG#v}"
    
    # Simple version comparison using sort
    HIGHER=$(printf '%s\n%s' "$VERSION" "$LATEST_VERSION" | sort -V | tail -1)
    
    if [ "$VERSION" = "$LATEST_VERSION" ]; then
        echo ""
        echo "❌ Error: Version $VERSION already exists as tag $LATEST_TAG"
        echo ""
        echo "💡 Suggestion: Bump the version in pyproject.toml"
        echo "   Current: version = \"$VERSION\""
        
        # Suggest next version
        IFS='.' read -r major minor patch <<< "$VERSION"
        NEXT_PATCH=$((patch + 1))
        echo "   Next:    version = \"$major.$minor.$NEXT_PATCH\""
        echo ""
        exit 1
    elif [ "$HIGHER" = "$LATEST_VERSION" ]; then
        echo ""
        echo "⚠️  Warning: Current version ($VERSION) is LOWER than latest tag ($LATEST_TAG)"
        echo ""
        echo "💡 Suggestion: Update pyproject.toml to a newer version"
        echo "   Latest:  $LATEST_VERSION"
        IFS='.' read -r major minor patch <<< "$LATEST_VERSION"
        NEXT_PATCH=$((patch + 1))
        echo "   Next:    $major.$minor.$NEXT_PATCH"
        echo ""
        read -p "Continue anyway? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "❌ Deployment cancelled"
            exit 1
        fi
    else
        echo "✅ Version validation passed ($VERSION > $LATEST_VERSION)"
    fi
fi
echo ""

# Check if tag already exists locally
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "⚠️  Warning: Tag $TAG already exists locally"
    read -p "Do you want to delete and recreate it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git tag -d "$TAG"
        echo "🗑️  Deleted local tag $TAG"
    else
        echo "❌ Deployment cancelled"
        exit 1
    fi
fi

read -p "Proceed with deployment? (y/n) " -n 1 -r
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
    echo "🎯 GitHub Actions CI workflow will be triggered automatically"
    echo "   View status at: https://github.com/$(git config --get remote.origin.url | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"
    echo ""
    echo "✨ Deployment complete!"
else
    echo "❌ Error: Failed to push tag"
    echo "   You may need to delete the remote tag: git push origin :refs/tags/$TAG"
    exit 1
fi
