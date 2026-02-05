# Dockerfile.cli
# Purpose: A sandbox container for Remoroo experiment execution.
# Includes build tools for packages with C extensions.

FROM python:3.11-slim

WORKDIR /app

# Install git and common build tools needed for packages with C extensions
# - build-essential: gcc, g++, make
# - swig: needed for box2d-py, etc.
# - cmake: needed for some ML packages
# - python3-dev: Python headers for building extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    swig \
    cmake \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Enable pip cache for faster subsequent installs
# (removed --no-cache-dir)
RUN pip install --upgrade pip

# Note: The CLI is not pre-installed. The sandbox mounts the repo and runs commands directly.

ENTRYPOINT ["/bin/bash"]
