# Environment Directory

This directory contains environment configuration files and computational methods for the dsHPC system.

## Configuration Files

The following configuration files define the computational environment:

### python.json

Configures the Python environment and packages:
```json
{
  "python_version": "3.10.0",
  "libraries": {
    "numpy": "1.24.0",
    "pandas": "2.0.0",
    "scipy": "",
    "scikit-learn": ""
  }
}
```

### r.json

Configures the R environment and packages:
```json
{
  "r_version": "4.3.0",
  "packages": {
    "dplyr": "",
    "ggplot2": "",
    "tidyverse": ""
  }
}
```

### system_deps.json

Configures additional system dependencies:
```json
{
  "apt_packages": [
    "libxml2-dev",
    "libcurl4-openssl-dev"
  ]
}
```

## Methods Directory

The `methods/` subdirectory contains computational methods that can be executed:

### methods/commands/
JSON files defining method metadata and parameters

### methods/scripts/
Implementation scripts for the methods (Python, R, etc.)

## Usage

1. Place your configuration files in this directory
2. The files will be read at container startup
3. Methods will be automatically loaded from the methods/ subdirectory
4. Changes require container restart to take effect

## Notes

- All configuration files are optional
- If not provided, default empty configurations will be used
- Methods are dynamically loaded and can be added/modified at runtime
