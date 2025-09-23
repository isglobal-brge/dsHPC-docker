# Scripts Directory

This directory contains the actual implementation code for computational methods in the dsHPC system.

## Purpose

Each subdirectory contains the executable code for a method defined in the `commands/` directory.

## Structure

Each method implementation follows this pattern:
```
scripts/
└── [method_name]/              # Directory name must match JSON file
    ├── main.py                 # Main execution script
    ├── requirements.txt        # Python dependencies (optional)
    ├── utils.py               # Helper modules (optional)
    └── README.md              # Method-specific documentation (optional)
```

## Main Script Requirements

The main execution script (specified in the JSON command file) must:

1. **Accept command-line arguments** for all parameters defined in the JSON
2. **Handle file inputs** properly (files are provided as paths)
3. **Produce output** in the expected format
4. **Exit with appropriate codes** (0 for success, non-zero for errors)
5. **Handle errors gracefully** with meaningful error messages

## Supported Languages

### Python (Recommended)
- Use `python3` as the command in JSON
- Add dependencies to `requirements.txt`
- Follow standard Python argument parsing

### R
- Use `Rscript` as the command in JSON
- Install packages via the R configuration
- Use command line argument parsing

### Shell Scripts
- Use `bash` or `sh` as the command
- Make scripts executable
- Handle arguments with `$1`, `$2`, etc.

## File Handling

- Input files are provided as absolute paths
- Output files should be written to the current working directory
- Use descriptive output filenames
- Clean up temporary files

## Error Handling

- Return exit code 0 for success
- Return non-zero exit codes for errors
- Write error messages to stderr
- Log important information to stdout

## Example Structure

```python
#!/usr/bin/env python3
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Method description')
    parser.add_argument('--param1', type=int, required=True)
    parser.add_argument('--input-file', type=str, required=True)
    
    args = parser.parse_args()
    
    try:
        # Your method logic here
        result = process_data(args.input_file, args.param1)
        print(f"Result: {result}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())
```

## Best Practices

1. **Validate inputs** at the start of your script
2. **Use meaningful variable names** and add comments
3. **Handle edge cases** and unexpected inputs
4. **Provide progress feedback** for long-running operations
5. **Follow language-specific conventions**
6. **Test thoroughly** with various input combinations
7. **Document your code** and algorithms used

## Testing

Test your methods locally before deployment:
```bash
cd scripts/your_method/
python3 main.py --param1 value --input-file test_file.txt
```

## Dependencies

- **Python**: Add to `requirements.txt`
- **R**: Configure in `/environment/r.json`
- **System**: Configure in `/environment/system_deps.json`
