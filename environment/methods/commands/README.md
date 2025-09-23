# Commands Directory

This directory contains JSON files that define method metadata and execution parameters for the dsHPC system.

## Purpose

Each JSON file in this directory defines:
- Method name and description
- Input parameters and their types
- Execution command and script path
- Version information
- Validation rules

## File Format

Each command file should be named `[method_name].json` and follow this structure:

```json
{
  "name": "method_name",
  "description": "Brief description of what this method does",
  "command": "python3",
  "script_path": "main.py",
  "parameters": [
    {
      "name": "parameter_name",
      "description": "Parameter description",
      "type": "integer|string|float|boolean|file",
      "required": true,
      "default": null
    }
  ],
  "version": "1.0.0"
}
```

## Parameter Types

- **integer**: Whole numbers
- **float**: Decimal numbers  
- **string**: Text values
- **boolean**: true/false values
- **file**: File upload parameters

## Required Fields

- `name`: Must match the script directory name
- `description`: User-friendly description
- `command`: Execution command (python3, Rscript, bash, etc.)
- `script_path`: Path to main script relative to method directory
- `parameters`: Array of parameter definitions
- `version`: Semantic version string

## Validation

- JSON files are validated at startup
- Invalid methods are skipped with warnings
- Method names must be unique
- Script paths must exist in corresponding scripts directory

## Best Practices

1. Use descriptive method names (lowercase, underscores)
2. Provide clear parameter descriptions
3. Set appropriate default values
4. Use semantic versioning
5. Test your JSON syntax before deployment
