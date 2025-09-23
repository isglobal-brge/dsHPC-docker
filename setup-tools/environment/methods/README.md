# Methods Directory

This directory contains computational methods that can be executed by the dsHPC system.

## Structure

The methods directory is organized into two subdirectories:

### commands/
Contains JSON files that define method metadata, parameters, and execution details.

### scripts/
Contains the actual implementation code for the methods (Python, R, shell scripts, etc.).

## How Methods Work

1. **Method Definition**: Each method is defined by a JSON file in `commands/`
2. **Method Implementation**: The actual code goes in `scripts/[method_name]/`
3. **Automatic Loading**: Methods are automatically discovered and loaded when the container starts

## Method Structure

For a method named `my_method`:
```
methods/
├── commands/
│   └── my_method.json          # Method definition
└── scripts/
    └── my_method/              # Method implementation directory
        ├── main.py             # Main execution script
        └── requirements.txt    # Dependencies (optional)
```

## Adding New Methods

1. Create a JSON definition file in `commands/`
2. Create a directory in `scripts/` with the same name
3. Add your implementation code
4. Restart the container to load the new method

## Supported Languages

- Python
- R
- Shell scripts
- Any language that can be executed from command line

## Notes

- Method names must match between the JSON file and script directory
- All methods are automatically validated and registered at startup
- Changes to methods require a container restart to take effect
