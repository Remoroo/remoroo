# Remoroo CLI

The `remoroo` CLI allows you to run the Remoroo offline engine interactively.

## Installation

```bash
pip install .
```

## Usage

### Run Local

To start a new run on the repository in the current directory:

```bash
remoroo run --local
```

### Options

- `--repo PATH`: Specify a different repository path (default: `.`)
- `--out PATH`: Specify output directory (default: `repo/runs`)
- `--yes`: Skip confirmation prompt
- `--verbose`: Show extended output

### Authentication

On the first run, `remoroo` will attempt to open your browser to sign in.
You will need to paste the API key back into the terminal.
Credentials are stored securely in `~/.config/remoroo/credentials`.

### Interactive Mode

The CLI will prompt you for:
1. **Goal**: What you want to achieve with this run.
2. **Metrics**: Key metrics to track (enter one per line).
