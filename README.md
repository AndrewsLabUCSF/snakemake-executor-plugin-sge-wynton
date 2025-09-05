# **Snakemake Executor Plugin for SGE Wynton**

### Overview

This plugin allows Snakemake to submit jobs to an SGE (Sun Grid Engine) cluster, specifically configured for the Wynton cluster at UCSF. This documentation will guide you through the usage and configuration of this plugin.

## **Prerequisites**

Before you begin, ensure that you have the following installed

- [Mamba/Conda](https://andrewslabucsf.github.io/Lab-Handbook/scripts/compute.html#installing-mamba-and-snakemake-detailed-instructions)
- [**Snakemake](https://andrewslabucsf.github.io/Lab-Handbook/scripts/compute.html#snakemake) (**inside the conda environement)
- **Python 3.11 or higher (**To Check : python —version**)**
- Poetry for managing dependencies (inside the conda environment)

## Installation

<aside>
💡

Remember to do this inside the conda environment so it doesn’t effect other installations you already have

</aside>

```jsx
conda create --name <env_name>

conda activate <env_name>
```

### **Step 1: Clone the Repository**

Clone the repository from GitHub (most updated branch : **beta-v1**):

```jsx
git clone git@github.com:AndrewsLabUCSF/snakemake-executor-plugin-sge-wynton.git
```

Then change to the most updated branch:

```jsx
git checkout beta-v1
```

### [Step 2: Install Snakemake (if not already installed)](https://andrewslabucsf.github.io/Lab-Handbook/scripts/compute.html#snakemake)

Although Snakemake is listed as a development dependency, you should ensure it is available in your environment. 

### Step 3: Install Poetry

If you do not have Poetry installed, you can install it using the following command:

```bash
curl -sSL https://install.python-poetry.org | python3 -

```

Add Poetry to your PATH if not already added by poetry itself (you might need to add this to your shell configuration file like .bashrc or .zshrc):

```bash
export PATH="$HOME/.local/bin:$PATH"

Then next command is :

source ~/.bashrc
```

### Step 4: Install Dependencies

Go to the directory where pyproject.toml is located.

```jsx
cd /wynton/home/andrews/xxx/snakemake-executor-plugin-sge-wynton
```

Use Poetry to install the project dependencies:

```bash
poetry install

```

This command creates a virtual environment and installs all the required dependencies specified in the pyproject.toml file. 

### Step 5: Change the [jobscript.sh](http://jobscript.sh) (need to change this manually for sge )

Go to the snakemake_interface_executor_plugin site package where you installed snakemake to change the jobscript.sh.

For example:

```jsx
nano /wynton/home/andrews/rakshyasharma/mambaforge/envs/{conda env name e.g: smk9}/lib/python3.12/site-packages/snakemake_interface_executor_plugins/executors/jobscript.sh
```

Delete whats in line 1 and Add this line in line 1

```jsx
#$ -S /bin/bash
```

Save your changes.

```jsx
conda deactivate
```

Then, reactivate conda again using 

```jsx
conda activate snakemake
```

### Step 6: Copy Test file to your user group directory

```jsx
cp -r /wynton/group/andrews/bin/genetic_correlations <path/to/your/directory>
```

### Step 7: Configuring Snakemake Workflow

`cd` into the test directory and ensure your Snakefile is correctly configured to use the resources specified for the SGE Wynton cluster.

<aside>
💡

You can use tmux sessions here

</aside>

### Step 8: Example Command

You can use the plugin with Snakemake by specifying it with the --executor flag. Below is an example of how to run a Snakemake workflow using the SGE Wynton executor:

```bash
snakemake --executor sge-wynton -j 6 --max-jobs-per-second 1 --use-conda --use-singularity
```

## Snakemake Unlock

If you get this error

> “LockException:
Error: Directory cannot be locked. Please make sure that no other Snakemake process is trying to create the same files in the following directory:…..”
> 

Run this command in your terminal

```jsx
snakemake --unlock
```

## Useful commands

```bash
poetry show

poetry update

```
