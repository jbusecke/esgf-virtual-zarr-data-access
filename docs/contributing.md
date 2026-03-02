---
title: Contributing Guide
---

# Contributing Guide

## Setting Up the Environment

```bash
uv sync
```

## Linting and Formatting

Pre-commit hooks run automatically on commit using [Ruff](https://docs.astral.sh/ruff/). To run manually:

```bash
uv run ruff check --fix
uv run ruff format
```

Install pre-commit hooks once:

```bash
uv run pre-commit install
```

## Building the Docs Locally

### Install MyST

The docs are built with [MyST-MD](https://mystmd.org). Install it (requires Node.js 20+):

```bash
npm install -g mystmd
```

Or via pip (bundles a Node.js runtime automatically):

```bash
pip install mystmd
```

To add it to the project's `docs` dependency group:

```bash
uv add --group docs mystmd
uv sync --group docs
```

### Start a Local Preview Server

From the project root:

```bash
myst start
```

This launches a live-reloading dev server at <http://localhost:3000>.

### Build Static HTML

```bash
myst build --html
```

Output is written to `./_build/html/`.

## Adding New Pages

1. Create a new `.md` file under `docs/` (or a `.ipynb` notebook under `notebooks/`).
2. Add the file path to the `toc` section in `myst.yml`:

```yaml
project:
  toc:
    - file: docs/index.md
    - file: docs/getting-started.md
    - file: docs/your-new-page.md   # add here
    - file: docs/contributing.md
```

## Notebooks

Notebooks are rendered **statically** from stored cell outputs — they are **not re-executed** at build time. This is intentional: the example notebooks require a live Dask cluster and access to ESGF/S3 data sources that are not available in CI.

When adding or updating a notebook:
1. Run it locally to completion so that output cells are populated.
2. Commit the notebook with outputs included.

## GitHub Pages Deployment

The docs are automatically built and deployed to GitHub Pages on every push to `main` via `.github/workflows/deploy-docs.yml`.

**One-time setup** (required after forking or first setup):
1. Go to the repository **Settings → Pages**.
2. Under **Build and deployment**, set the source to **GitHub Actions**.

The deployed site will be available at:
`https://jbusecke.github.io/esgf-virtual-zarr-data-access/`
