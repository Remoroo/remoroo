# Dockerfile.cli
# Purpose: A lightweight distribution container for the Remoroo CLI.
# Target Size: < 250MB

FROM python:3.11-slim

WORKDIR /app

# Install git as it's needed for the local worker to manage repos
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install CLI directly from the new GitHub repository
# This ensures that we only pull the code in that repo (no remoroo_brain)
RUN pip install --no-cache-dir git+https://github.com/Remoroo/remoroo.git@main

# Clean up
RUN apt-get purge -y --auto-remove && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["remoroo"]
