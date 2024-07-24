# Snakemake Executor Plugin for SGE Wynton

## Overview

This plugin allows Snakemake to submit jobs to an SGE (Sun Grid Engine) cluster, specifically configured for the Wynton cluster at UCSF. This documentation will guide you through the usage and configuration of this plugin.

## Prerequisites

Before you begin, ensure that you have the following installed:

	•	Python 3.7 or higher
	•	snakemake
	•	Poetry (for managing dependencies)
 
 
## Installation

### Step 1: Clone the Repository

Clone the repository from GitHub:
```bash
git clone git@github.com:AndrewsLabUCSF/snakemake-executor-plugin-sge-wynton.git
```
```bash
cd snakemake-executor-plugin-sge-wynton
```

### Step 2: Install Poetry

If you do not have Poetry installed, you can install it using the following command:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```
Add Poetry to your PATH (you might need to add this to your shell configuration file like .bashrc or .zshrc):
```bash
export PATH="$HOME/.local/bin:$PATH"
```

### Step 3: Install Dependencies

Use Poetry to install the project dependencies:

```bash
poetry install
```
This command creates a virtual environment and installs all the required dependencies specified in the pyproject.toml file.

### Step 4: Install Snakemake (if not already installed)
Although Snakemake is listed as a development dependency, you should ensure it is available in your environment:
```bash
poetry run pip install snakemake
```
### Step 5: Configuring Snakemake Workflow

Ensure your Snakefile is correctly configured to use the resources specified for the SGE Wynton cluster.

### Step 6: Example Command

You can use the plugin with Snakemake by specifying it with the --executor flag. Below is an example of how to run a Snakemake workflow using the SGE Wynton executor:
```bash
snakemake --executor sge-wynton --jobs 3 --forceall
```
## Useful commands
```bash
poetry show
```
```bash
poetry update
```
