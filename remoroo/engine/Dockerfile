# Dockerfile.cli
# Purpose: A sandbox container for Remoroo experiment execution.
# Includes build tools for packages with C extensions.
#
# Build args:
#   BASE_IMAGE   — override the base (default: python:3.11-slim)
#   TORCH_INDEX  — PyTorch wheel index: CPU or CUDA variant

ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

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
RUN pip install --upgrade pip

# PyTorch — the index URL selects CPU or CUDA wheels at build time.
ARG TORCH_INDEX=https://download.pytorch.org/whl/cpu
RUN pip install torch --index-url ${TORCH_INDEX}

# Common ML/data deps used by autoresearch and similar workloads
RUN pip install tiktoken pyarrow requests matplotlib numpy pandas

# uv package manager — required by autoresearch for `uv run` execution
RUN pip install uv

ENTRYPOINT ["/bin/bash"]
