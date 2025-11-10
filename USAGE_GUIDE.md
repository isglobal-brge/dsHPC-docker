# Complete Guide: Using dsHPC Package to Submit Jobs, Meta-Jobs, and Pipelines

This comprehensive guide explains how to use the **dsHPC R package** to submit jobs, meta-jobs, and pipelines. It covers all input types: single files, multiple files (named inputs), multiple files (arrays), and parameters-only jobs.

**This guide is for end users of the dsHPC package.** All examples use functions from the `dsHPC` package.

## Table of Contents

1. [Introduction](#introduction)
2. [Quick Start: How to Know What Your Method Needs](#quick-start-how-to-know-what-your-method-needs)
3. [Understanding Input Types](#understanding-input-types)
4. [Understanding Parameters](#understanding-parameters)
5. [Jobs: Complete Examples](#jobs-complete-examples)
6. [Meta-Jobs: Complete Examples](#meta-jobs-complete-examples)
7. [Pipelines: Complete Examples](#pipelines-complete-examples)
8. [Reference System ($ref)](#reference-system-ref)
9. [Troubleshooting](#troubleshooting)
10. [Method Script Development: Handling Input Formats](#method-script-development-handling-input-formats)
11. [Array Ordering and Deduplication](#array-ordering-and-deduplication)
12. [Best Practices](#best-practices)
13. [Function Reference](#function-reference)

---

## Introduction

### What is dsHPC?

dsHPC is an R package that provides a simple interface to submit computational jobs to a distributed HPC system. You can:

- **Submit single jobs** - Process one file with one method
- **Submit meta-jobs** - Chain multiple methods sequentially
- **Submit pipelines** - Create DAGs (Directed Acyclic Graphs) of meta-jobs for parallel processing

### Input Types Supported

The dsHPC system supports **four ways** to provide inputs:

1. **Single File (Legacy)**: One file using `file_hash` or `content`
2. **Multi-File Named Inputs**: Multiple named files using `file_inputs` (e.g., `input_a`, `input_b`)
3. **Multi-File Arrays**: Arrays of files within `file_inputs` (e.g., `inputs = [hash1, hash2, hash3]`)
4. **Parameters Only**: No files, only parameters

### Key Rules

1. **Exactly one of `file_hash` OR `file_inputs`**: You cannot use both at the same time. Use `NULL` for the one you're not using.
2. **Both can be NULL**: For params-only jobs (jobs that only use parameters, no files).
3. **Arrays preserve order**: The order of elements in an array is important and preserved exactly as you specify.
4. **Automatic deduplication**: The system automatically detects identical jobs/meta-jobs/pipelines and reuses existing results.

### Which Function Should I Use?

| Your Situation | Use This Function |
|---------------|-------------------|
| **Single file** (have content or file path) | `query_job()` or `wait_for_job_results()` |
| **Single file** (have hash already) | `query_job_by_hash()` or `wait_for_job_results_by_hash()` |
| **Multiple files** (named inputs or arrays) | `query_job_by_hash()` with `file_inputs` parameter |
| **Params-only job** | `query_job_by_hash()` with `file_hash = NULL` and `file_inputs = NULL` |
| **Meta-job with single file** | `submit_meta_job()` or `execute_processing_chain()` |
| **Meta-job with single file hash** | `submit_meta_job_by_hash()` or `execute_processing_chain_by_hash()` |
| **Meta-job with multiple files** | `submit_meta_job_multi()` or `execute_processing_chain_multi()` |
| **Pipeline** | `submit_pipeline()` or `execute_pipeline()` |

---

## Quick Start: How to Know What Your Method Needs

**Before submitting any job, you MUST check what the method expects.** This is the most important step to avoid errors.

### Step 1: Get Method Information Using dsHPC Package

The easiest way is to use the `get_methods()` function:

```r
library(dsHPC)
config <- create_api_config("http://localhost", 8001, "your_api_key")

# Get all available methods
methods <- get_methods(config)

# Check a specific method
concat_info <- methods$concat
print(concat_info)
```

### Step 2: Check Method Definition JSON (Alternative)

Each method has a JSON definition file that tells you exactly what inputs it expects. These files are typically located in:
```
environment/methods/commands/<method_name>.json
```

### Step 3: Understand What the Method Expects

Look for the `input_modes.file_inputs` array. This tells you:
- What input names the method expects
- Whether each input is a single file or an array
- Whether each input is required

### Example 1: Named Inputs Method (`concat`)

**Using `get_methods()`:**
```r
methods <- get_methods(config)
concat_info <- methods$concat
# Check input_modes.file_inputs
```

**Or check JSON file**: `environment/methods/commands/concat.json`

```json
{
  "name": "concat",
  "input_modes": {
    "file_inputs": [
      {
        "name": "input_a",
        "type": "text/json",
        "required": true
      },
      {
        "name": "input_b",
        "type": "text/json",
        "required": true
      }
    ]
  }
}
```

**What this means:**
- The method expects **two named inputs**: `input_a` and `input_b`
- Each input gets **ONE file** (not an array)
- Both inputs are **required**

**How to provide inputs:**
```r
file_inputs <- list(
  input_a = "hash1",  # ONE file hash (string)
  input_b = "hash2"   # ONE file hash (string)
)
```

### Example 2: Array Input Method (`concat_multi`)

**JSON file**: `environment/methods/commands/concat_multi.json`

```json
{
  "name": "concat_multi",
  "input_modes": {
    "file_inputs": [
      {
        "name": "inputs",
        "type": "array",
        "required": true
      }
    ]
  }
}
```

**What this means:**
- The method expects **ONE input name**: `inputs`
- This input gets **MULTIPLE files** (an array)
- The input is **required**

**How to provide inputs:**
```r
file_inputs <- list(
  inputs = c("hash1", "hash2", "hash3")  # Array of file hashes
)
```

### Example 3: Single File Method (Legacy)

Some methods support single file input via `file_hash`. Check `input_modes.supports_single_file`:

```json
{
  "name": "some_method",
  "input_modes": {
    "supports_single_file": true
  }
}
```

**How to provide input:**
```r
# Option 1: Provide content directly
query_job(config, content, "some_method", list())

# Option 2: Provide file_hash
query_job_by_hash(config, file_hash = "hash1", "some_method", list())
```

### Example 4: Parameters-Only Method

Some methods don't need files, only parameters:

```json
{
  "name": "generate_data",
  "input_modes": {
    "supports_no_input": true
  },
  "parameters": [
    {
      "name": "count",
      "type": "integer",
      "required": true
    }
  ]
}
```

**How to provide:**
```r
query_job_by_hash(
  config,
  file_hash = NULL,        # No file
  method_name = "generate_data",
  parameters = list(count = 100),
  file_inputs = NULL       # No file_inputs
)
```

### Quick Reference Table

| Method JSON Shows | What It Means | How to Provide |
|------------------|---------------|----------------|
| Multiple entries in `file_inputs` array | Named inputs - each gets ONE file | `list(input_a = "hash1", input_b = "hash2")` |
| One entry with `"type": "array"` | Array input - ONE name gets MULTIPLE files | `list(inputs = c("hash1", "hash2", "hash3"))` |
| `"supports_single_file": true` | Can use `file_hash` (legacy) | `file_hash = "hash1"` or `content` |
| `"supports_no_input": true` | No files needed, only parameters | `file_hash = NULL`, `file_inputs = NULL` |

### Common Mistakes to Avoid

❌ **Wrong**: Providing an array when method expects named inputs
```r
# Method expects input_a and input_b (named inputs)
file_inputs <- list(
  inputs = c("hash1", "hash2")  # WRONG! Method doesn't have "inputs"
)
```

✅ **Correct**: Providing named inputs matching method definition
```r
file_inputs <- list(
  input_a = "hash1",  # Correct!
  input_b = "hash2"   # Correct!
)
```

❌ **Wrong**: Providing named inputs when method expects an array
```r
# Method expects inputs (array)
file_inputs <- list(
  input_a = "hash1",  # WRONG! Method doesn't have "input_a"
  input_b = "hash2"   # WRONG! Method doesn't have "input_b"
)
```

✅ **Correct**: Providing an array matching method definition
```r
file_inputs <- list(
  inputs = c("hash1", "hash2")  # Correct!
)
```

---

## Understanding Input Types

There are **three distinct patterns** for providing files. Understanding the difference is crucial.

### Pattern 1: Single File (Legacy)

**When to use**: Methods that accept a single file input.

**Two ways to provide:**

**Option A: Provide content directly** (dsHPC calculates hash automatically)
```r
content <- "Hello, World!"
result <- query_job(config, content, "some_method", list())
```

**Option B: Provide file_hash** (you've already uploaded and have the hash)
```r
file_hash <- "abc123def456..."
result <- query_job_by_hash(config, file_hash = file_hash, "some_method", list())
```

**How it works:**
- File is downloaded to: `workspace/input/`
- Method script receives single file path in `metadata.json`

### Pattern 2: Named Inputs (Multiple Input Names, Each Gets One File)

**Visual Diagram:**
```
Method expects:          input_a    input_b
You provide:            hash1      hash2
Structure:              {input_a: "hash1", input_b: "hash2"}
```

**When to use**: Methods that expect multiple **named** parameters, each with one file.

**Example method**: `concat` expects `input_a` and `input_b`, each with one file.

**R code:**
```r
file_inputs <- list(
  input_a = "hash1",  # ONE file for input_a
  input_b = "hash2"   # ONE file for input_b
)

result <- query_job_by_hash(
  config,
  file_hash = NULL,              # Must be NULL when using file_inputs
  method_name = "concat",
  parameters = list(),
  file_inputs = file_inputs
)
```

**How it works:**
- Files are downloaded to: `workspace/input_a/` and `workspace/input_b/`
- Method script receives in `metadata.json`:
  ```json
  {
    "files": {
      "input_a": "/path/to/workspace/input_a/file.json",
      "input_b": "/path/to/workspace/input_b/file.json"
    }
  }
  ```

### Pattern 3: Array Input (One Input Name Gets Multiple Files)

**Visual Diagram:**
```
Method expects:          inputs
You provide:            [hash1, hash2, hash3]
Structure:              {inputs: ["hash1", "hash2", "hash3"]}
```

**When to use**: Methods that expect **one** input parameter that accepts **multiple** files as an array.

**Example method**: `concat_multi` expects `inputs` (array), which can contain multiple files.

**R code:**
```r
file_inputs <- list(
  inputs = c("hash1", "hash2", "hash3")  # MULTIPLE files for ONE input name
)

result <- query_job_by_hash(
  config,
  file_hash = NULL,
  method_name = "concat_multi",
  parameters = list(separator = "|"),
  file_inputs = file_inputs
)
```

**How it works:**
- Files are downloaded to: `workspace/inputs/0/`, `workspace/inputs/1/`, `workspace/inputs/2/`
- Method script receives in `metadata.json`:
  ```json
  {
    "files": {
      "inputs": [
        "/path/to/workspace/inputs/0/file.json",
        "/path/to/workspace/inputs/1/file.json",
        "/path/to/workspace/inputs/2/file.json"
      ]
    }
  }
  ```

### Pattern 4: Parameters Only (No Files)

**When to use**: Methods that only need parameters and no input files.

**R code:**
```r
result <- query_job_by_hash(
  config,
  file_hash = NULL,              # NULL - no file
  method_name = "generate_data",
  parameters = list(count = 100, seed = 42),
  file_inputs = NULL              # NULL - no file_inputs
)
```

### Comparison Table

| Aspect | Single File | Named Inputs | Array Input | Params Only |
|--------|------------|-------------|-------------|-------------|
| **Structure** | `file_hash = "hash"` | `{input_a: "hash1", input_b: "hash2"}` | `{inputs: ["hash1", "hash2"]}` | `file_hash = NULL`, `file_inputs = NULL` |
| **File paths** | `workspace/input/` | `workspace/input_a/`, `workspace/input_b/` | `workspace/inputs/0/`, `workspace/inputs/1/` | N/A |
| **Use case** | Single file methods | Methods with multiple named parameters | Methods with one array parameter | Methods that don't need files |
| **Function** | `query_job()` or `query_job_by_hash()` | `query_job_by_hash()` with `file_inputs` | `query_job_by_hash()` with `file_inputs` | `query_job_by_hash()` |

### Mixed Usage

You can mix named inputs and arrays in the same `file_inputs`:

```r
file_inputs <- list(
  primary = "hash1",                    # Named input (single file)
  secondary = "hash2",                  # Named input (single file)
  references = c("hash3", "hash4", "hash5")  # Array input (multiple files)
)
```

This creates:
- `workspace/primary/` (one file)
- `workspace/secondary/` (one file)
- `workspace/references/0/`, `workspace/references/1/`, `workspace/references/2/` (three files)

---

## Understanding Parameters

Parameters are method-specific configuration options that control how the method processes your data.

### How to Find What Parameters a Method Accepts

**Using dsHPC package:**
```r
methods <- get_methods(config)
method_info <- methods$concat_multi

# Check parameters
print(method_info$parameters)
```

**Or check the method JSON file:**
```json
{
  "name": "concat_multi",
  "parameters": [
    {
      "name": "separator",
      "type": "string",
      "description": "Separator between concatenated texts",
      "required": false,
      "default": ""
    }
  ]
}
```

### Parameter Types

Parameters can be:
- **Strings**: `"value"`
- **Numbers**: `42` or `3.14`
- **Booleans**: `TRUE` or `FALSE`
- **Lists/Arrays**: `list("a", "b", "c")` or `c("a", "b", "c")`
- **Named lists**: `list(key1 = "value1", key2 = "value2")`

### Providing Parameters

Parameters are always provided as a **named list**:

```r
parameters <- list(
  separator = "|",        # String parameter
  threshold = 50,         # Number parameter
  verbose = TRUE,         # Boolean parameter
  options = list(a = 1, b = 2)  # Nested list parameter
)
```

### Required vs Optional Parameters

- **Required parameters**: Must be provided, otherwise the job will fail
- **Optional parameters**: Can be omitted, method will use default values

**Example:**
```r
# Method requires 'count' (required) and accepts 'seed' (optional)
parameters <- list(
  count = 100,    # Required - must provide
  seed = 42       # Optional - can omit if you want default
)
```

### Validating Parameters

The dsHPC package can validate parameters before submission:

```r
# Validate parameters (default: TRUE)
result <- query_job_by_hash(
  config,
  file_hash = NULL,
  method_name = "some_method",
  parameters = list(count = 100),
  validate_parameters = TRUE  # Validates against method spec
)
```

If validation fails, you'll get an error message explaining what's wrong:
- Missing required parameters
- Unexpected parameters provided
- Wrong parameter types

### Parameter Examples

**Example 1: String parameter**
```r
parameters <- list(separator = "|")
```

**Example 2: Number parameter**
```r
parameters <- list(threshold = 50, max_iterations = 1000)
```

**Example 3: Boolean parameter**
```r
parameters <- list(verbose = TRUE, normalize = FALSE)
```

**Example 4: Array parameter**
```r
parameters <- list(features = c("feature1", "feature2", "feature3"))
```

**Example 5: Nested parameters**
```r
parameters <- list(
  config = list(
    mode = "advanced",
    options = list(option1 = TRUE, option2 = FALSE)
  )
)
```

**Example 6: Empty parameters (no parameters)**
```r
parameters <- list()  # Empty list
```

---

## Jobs: Complete Examples

### Example 1: Single File Job (Using Content)

**Step 1: Check Method Definition**

```r
methods <- get_methods(config)
# Check if method supports single file input
```

**Step 2: Prepare Content**

```r
library(dsHPC)
config <- create_api_config("http://localhost", 8001, "your_api_key")

# Prepare your content (can be raw bytes, character string, or R object)
content <- "Hello, World!"
# Or: content <- charToRaw(jsonlite::toJSON(list(data = list(text = "Hello"))))
```

**Step 3: Submit Job**

```r
# Option A: Submit and wait for results in one call
result <- wait_for_job_results(
  config,
  content = content,
  method_name = "some_method",
  parameters = list(),
  timeout = 60,
  parse_json = TRUE
)

# Option B: Submit first, then wait separately
job_response <- query_job(config, content, "some_method", list())
# Later...
output <- wait_for_job_results_by_hash(
  config,
  file_hash = job_response$file_hash,
  method_name = "some_method",
  parameters = list(),
  timeout = 60,
  parse_json = TRUE
)
```

**Step 4: Extract Result**

```r
output_text <- result$data$text
cat(sprintf("Result: %s\n", output_text))
```

### Example 2: Single File Job (Using File Hash)

**Step 1: Upload File First**

```r
# Upload file and get hash
file_content <- jsonlite::toJSON(list(data = list(text = "Hello")), auto_unbox = TRUE)
file_hash <- upload_file(config, content = charToRaw(file_content), filename = "input.json")
```

**Step 2: Submit Job Using Hash**

```r
# Option A: Submit and wait in one call
result <- wait_for_job_results_by_hash(
  config,
  file_hash = file_hash,
  method_name = "some_method",
  parameters = list(),
  timeout = 60,
  parse_json = TRUE
)

# Option B: Submit first, check status, then wait
job_response <- query_job_by_hash(config, file_hash = file_hash, "some_method", list())
status <- get_job_status_by_hash(config, file_hash, "some_method", list())
if (job_succeeded_by_hash(config, file_hash, "some_method", list())) {
  output <- get_job_output_by_hash(config, file_hash, "some_method", list(), parse_json = TRUE)
}
```

### Example 3: Job with Named Inputs

**Step 1: Check Method Definition**

```r
methods <- get_methods(config)
concat_info <- methods$concat
# Method expects: input_a and input_b (named inputs)
```

**Step 2: Upload Files**

```r
file1_content <- jsonlite::toJSON(list(data = list(text = "Hello")), auto_unbox = TRUE)
file2_content <- jsonlite::toJSON(list(data = list(text = "World")), auto_unbox = TRUE)

file1_hash <- upload_file(config, content = charToRaw(file1_content), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(file2_content), filename = "file2.json")
```

**Step 3: Construct file_inputs**

```r
# Match the method definition: input_a and input_b, each with ONE file
file_inputs <- list(
  input_a = file1_hash,  # Single file hash (string)
  input_b = file2_hash    # Single file hash (string)
)
```

**Step 4: Submit Job**

```r
result <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,              # Must be NULL when using file_inputs
  method_name = "concat",
  parameters = list(),
  file_inputs = file_inputs,
  timeout = 60,
  parse_json = TRUE
)
```

**Step 5: Verify Result**

```r
output_text <- result$data$text
# Expected: "HelloWorld"
cat(sprintf("Result: %s\n", output_text))
```

### Example 4: Job with Array Input

**Step 1: Check Method Definition**

```r
methods <- get_methods(config)
concat_multi_info <- methods$concat_multi
# Method expects: inputs (array)
```

**Step 2: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "!")))), filename = "file3.json")
```

**Step 3: Construct file_inputs**

```r
# Match the method definition: inputs is an ARRAY (multiple files)
file_inputs <- list(
  inputs = c(file1_hash, file2_hash, file3_hash)  # Array of hashes
)
```

**Step 4: Submit Job with Parameters**

```r
result <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,
  method_name = "concat_multi",
  parameters = list(separator = "|"),  # Method parameter
  file_inputs = file_inputs,
  timeout = 60,
  parse_json = TRUE
)
```

**Step 5: Verify Result**

```r
output_text <- result$data$text
# Expected: "Hello|World|!"
cat(sprintf("Result: %s\n", output_text))
```

### Example 5: Parameters-Only Job

**Step 1: Check Method Definition**

```r
methods <- get_methods(config)
generate_info <- methods$generate_data
# Method supports_no_input: true
# Parameters: count (required), seed (optional)
```

**Step 2: Submit Job**

```r
result <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,              # NULL - no file
  method_name = "generate_data",
  parameters = list(
    count = 100,                 # Required parameter
    seed = 42                    # Optional parameter
  ),
  file_inputs = NULL,            # NULL - no file_inputs
  timeout = 60,
  parse_json = TRUE
)
```

**Step 3: Verify Result**

```r
generated_data <- result$data
cat(sprintf("Generated %d items\n", length(generated_data)))
```

### Example 6: Checking Job Status

```r
# Submit job
job_response <- query_job_by_hash(
  config,
  file_hash = file_hash,
  method_name = "some_method",
  parameters = list()
)

# Check status
status <- get_job_status_by_hash(
  config,
  file_hash = file_hash,
  method_name = "some_method",
  parameters = list()
)
cat(sprintf("Job status: %s\n", status))

# Check if succeeded
if (job_succeeded_by_hash(config, file_hash, "some_method", list())) {
  # Get output
  output <- get_job_output_by_hash(
    config,
    file_hash = file_hash,
    method_name = "some_method",
    parameters = list(),
    parse_json = TRUE
  )
}
```

---

## Meta-Jobs: Complete Examples

Meta-jobs allow you to chain multiple methods sequentially, where the output of one step becomes the input of the next.

### Understanding Meta-Jobs

- **Initial Input**: Can be `content` (single file), `file_hash` (single file), or `initial_file_inputs` (multi-file)
- **Chain**: List of method steps executed sequentially
- **References**: Use `$ref:prev` to reference the output of the previous step
- **Intermediate file_inputs**: Steps can have their own `file_inputs` for additional files

### Example 1: Meta-Job with Single File (Using Content)

**Step 1: Prepare Content**

```r
content <- "Hello, World!"
```

**Step 2: Define Chain**

```r
chain <- list(
  # Step 1: Process initial content
  list(
    method_name = "process_step1",
    parameters = list()
  ),
  # Step 2: Use output from step 1
  list(
    method_name = "process_step2",
    parameters = list(),
    file_inputs = list(input = "$ref:prev")  # Reference to previous step
  )
)
```

**Step 3: Submit and Wait**

```r
# Option A: Submit and wait in one call
result <- execute_processing_chain(
  config,
  content = content,
  method_chain = chain,
  timeout = 120,
  parse_json = TRUE
)

# Option B: Submit first, then wait separately
meta_response <- submit_meta_job(config, content, chain)
result <- wait_for_meta_job_results(
  config,
  meta_response$meta_job_hash,
  timeout = 120,
  parse_json = TRUE
)
```

### Example 2: Meta-Job with Single File Hash

**Step 1: Upload File**

```r
file_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "input.json")
```

**Step 2: Define Chain**

```r
chain <- list(
  list(method_name = "concat", parameters = list(a = "Hello", b = "World")),
  list(
    method_name = "append_line",
    parameters = list(line = "Extra"),
    file_inputs = list(input = "$ref:prev")
  )
)
```

**Step 3: Submit and Wait**

```r
# Option A: Submit and wait in one call
result <- execute_processing_chain_by_hash(
  config,
  file_hash = file_hash,
  method_chain = chain,
  timeout = 120,
  parse_json = TRUE
)

# Option B: Submit first, then wait
meta_response <- submit_meta_job_by_hash(config, file_hash = file_hash, method_chain = chain)
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)
```

### Example 3: Meta-Job with Named Inputs

**Step 1: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
```

**Step 2: Construct initial_file_inputs**

```r
# First step expects input_a and input_b (named inputs)
initial_file_inputs <- list(
  input_a = file1_hash,
  input_b = file2_hash
)
```

**Step 3: Define Chain**

```r
chain <- list(
  # Step 1: Uses initial_file_inputs automatically (input_a and input_b)
  list(method_name = "concat", parameters = list()),
  
  # Step 2: Uses output from step 1 via $ref:prev
  list(
    method_name = "append_line",
    parameters = list(line = "Extra"),
    file_inputs = list(input = "$ref:prev")  # Reference to previous step
  )
)
```

**Step 4: Submit and Wait**

```r
# Option A: Submit and wait in one call
result <- execute_processing_chain_multi(
  config,
  file_inputs = initial_file_inputs,
  method_chain = chain,
  timeout = 120,
  parse_json = TRUE
)

# Option B: Submit first, then wait
meta_response <- submit_meta_job_multi(config, initial_file_inputs, chain)
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)
```

**Step 5: Verify Result**

```r
output_text <- result$data$text
# Expected: "HelloWorld\nExtra"
```

### Example 4: Meta-Job with Array Input

**Step 1: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "A")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "B")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "C")))), filename = "file3.json")
```

**Step 2: Construct initial_file_inputs**

```r
# First step expects inputs (array)
initial_file_inputs <- list(
  inputs = c(file1_hash, file2_hash, file3_hash)  # Array
)
```

**Step 3: Define Chain**

```r
chain <- list(
  list(method_name = "concat_multi", parameters = list(separator = "|"))
)
```

**Step 4: Submit and Wait**

```r
result <- execute_processing_chain_multi(
  config,
  file_inputs = initial_file_inputs,
  method_chain = chain,
  timeout = 120,
  parse_json = TRUE
)
```

**Step 5: Verify Result**

```r
output_text <- result$data$text
# Expected: "A|B|C"
```

### Example 5: Meta-Job with file_inputs in Intermediate Steps

**Step 1: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "!")))), filename = "file3.json")
```

**Step 2: Define initial_file_inputs**

```r
initial_file_inputs <- list(
  input_a = file1_hash,
  input_b = file2_hash
)
```

**Step 3: Define Chain with Intermediate file_inputs**

```r
chain <- list(
  # Step 1: Uses initial_file_inputs automatically
  list(method_name = "concat", parameters = list()),
  
  # Step 2: Uses file_inputs with array (new files, not from previous step)
  list(
    method_name = "concat_multi",
    parameters = list(separator = "|"),
    file_inputs = list(
      inputs = c(file1_hash, file2_hash, file3_hash)  # Array of files
    )
  )
)
```

**Step 4: Submit and Wait**

```r
result <- execute_processing_chain_multi(
  config,
  file_inputs = initial_file_inputs,
  method_chain = chain,
  timeout = 120,
  parse_json = TRUE
)
```

### Example 6: Checking Meta-Job Status

```r
# Submit meta-job
meta_response <- submit_meta_job_multi(config, initial_file_inputs, chain)

# Check status
status <- get_meta_job_status(config, meta_response$meta_job_hash)
cat(sprintf("Meta-job status: %s\n", status$status))
cat(sprintf("Current step: %d/%d\n", status$current_step, length(status$chain)))

# Wait for completion
result <- wait_for_meta_job_results(
  config,
  meta_response$meta_job_hash,
  timeout = 120,
  parse_json = TRUE
)
```

---

## Pipelines: Complete Examples

Pipelines allow you to create DAGs (Directed Acyclic Graphs) of meta-jobs, where multiple nodes can execute in parallel and their outputs can be combined.

### Understanding Pipelines

- **Nodes**: Each node is a meta-job (chain of methods)
- **Dependencies**: Nodes can depend on other nodes
- **Parallel Execution**: Nodes without dependencies run in parallel
- **Convergence**: Multiple nodes can converge into one final node
- **References**: Use `$ref:nodeX` to reference outputs from other nodes
- **file_inputs**: Nodes can have `file_inputs` in their first step

### Example 1: Simple Pipeline with Named Inputs

**Step 1: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
```

**Step 2: Create Pipeline Node**

```r
node1 <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat",
      parameters = list(),
      file_inputs = list(
        input_a = file1_hash,  # Named input
        input_b = file2_hash    # Named input
      )
    )
  ),
  dependencies = character(0)  # Root node (no dependencies)
)
```

**Step 3: Create and Submit Pipeline**

```r
pipeline <- list(nodes = list(node1 = node1))

# Option A: Submit and wait in one call
result <- execute_pipeline(
  config,
  pipeline_definition = pipeline,
  timeout = 120,
  parse_json = TRUE
)

# Option B: Submit first, then wait
pipeline_response <- submit_pipeline(config, pipeline)
result <- wait_for_pipeline(
  config,
  pipeline_response$pipeline_hash,
  timeout = 120,
  parse_json = TRUE
)
```

**Step 4: Verify Result**

```r
output_text <- result$final_output$data$text
# Expected: "HelloWorld"
```

### Example 2: Pipeline with Array Input

**Step 1: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "A")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "B")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "C")))), filename = "file3.json")
```

**Step 2: Create Pipeline Node**

```r
node1 <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat_multi",
      parameters = list(separator = "|"),
      file_inputs = list(
        inputs = c(file1_hash, file2_hash, file3_hash)  # Array
      )
    )
  ),
  dependencies = character(0)
)
```

**Step 3: Create and Submit Pipeline**

```r
pipeline <- list(nodes = list(node1 = node1))
result <- execute_pipeline(config, pipeline, timeout = 120, parse_json = TRUE)
output_text <- result$final_output$data$text
# Expected: "A|B|C"
```

### Example 3: Pipeline with Parallel Nodes

**Step 1: Upload Files**

```r
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "A")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "B")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "C")))), filename = "file3.json")
file4_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "D")))), filename = "file4.json")
```

**Step 2: Create Parallel Nodes**

```r
# Node A: Named inputs
node_a <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat",
      parameters = list(),
      file_inputs = list(
        input_a = file1_hash,
        input_b = file2_hash
      )
    )
  ),
  dependencies = character(0)  # No dependencies = root node (runs in parallel)
)

# Node B: Array input
node_b <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat_multi",
      parameters = list(separator = "|"),
      file_inputs = list(
        inputs = c(file3_hash, file4_hash)
      )
    )
  ),
  dependencies = character(0)  # No dependencies = root node (runs in parallel)
)
```

**Step 3: Create Final Node (Combines Outputs)**

```r
node_final <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat_multi",
      parameters = list(separator = " | "),
      file_inputs = list(
        inputs = list(
          "$ref:node_a/data/text",  # Reference to node_a output
          "$ref:node_b/data/text"    # Reference to node_b output
        )
      )
    )
  ),
  dependencies = c("node_a", "node_b")  # Depends on both nodes
)
```

**Step 4: Create and Submit Pipeline**

```r
pipeline <- list(
  nodes = list(
    node_a = node_a,
    node_b = node_b,
    node_final = node_final
  )
)

result <- execute_pipeline(config, pipeline, timeout = 180, parse_json = TRUE)
output_text <- result$final_output$data$text
# Expected: "AB | C|D"
```

### Example 4: Checking Pipeline Status

```r
# Submit pipeline
pipeline_response <- submit_pipeline(config, pipeline)

# Check status
status <- get_pipeline_status(config, pipeline_response$pipeline_hash)
cat(sprintf("Pipeline status: %s\n", status$status))
cat(sprintf("Progress: %.1f%% (%d/%d nodes)\n", 
            status$progress * 100,
            status$completed_nodes,
            status$total_nodes))

# Wait for completion
result <- wait_for_pipeline(
  config,
  pipeline_response$pipeline_hash,
  timeout = 300,
  parse_json = TRUE
)
```

---

## Reference System ($ref)

The `$ref` system allows you to reference outputs from previous steps (in meta-jobs) or other nodes (in pipelines).

### Reference Types

1. **`$ref:prev`**: Complete output from the previous step in a meta-job
2. **`$ref:prev/data/text`**: Extract the value at path `data.text` from the previous step output
3. **`$ref:node1`**: Complete output from node `node1` in a pipeline
4. **`$ref:node1/data/text`**: Extract the value at path `data.text` from node `node1` output

### How It Works

1. When a `$ref:` reference is found, it's resolved to a file hash
2. If there's a path (e.g., `/data/text`), that value is extracted from the JSON and a new file is created
3. The extracted file is stored in the database with its own hash
4. The extracted file hash is used as input for the next step

### Examples

#### In Meta-Jobs

```r
chain <- list(
  # Step 1: Process initial file
  list(method_name = "concat", parameters = list(a = "Hello", b = "World")),
  
  # Step 2: Use complete output from step 1
  list(
    method_name = "append_line",
    parameters = list(line = "Extra"),
    file_inputs = list(input = "$ref:prev")  # Complete output
  ),
  
  # Step 3: Extract specific path from step 2 output
  list(
    method_name = "count_chars",
    parameters = list(),
    file_inputs = list(input = "$ref:prev/data/text")  # Extract data.text
  )
)
```

#### In Pipelines

```r
# Node that depends on other nodes and uses their outputs
node_final <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat_multi",
      parameters = list(separator = "|"),
      file_inputs = list(
        inputs = list(
          "$ref:node_a/data/text",  # Extract data.text from node_a
          "$ref:node_b/data/text",   # Extract data.text from node_b
          "$ref:node_c"               # Complete output from node_c
        )
      )
    )
  ),
  dependencies = c("node_a", "node_b", "node_c")
)
```

#### Arrays with References

```r
# Array mixing references and direct hashes
file_inputs <- list(
  inputs = list(
    "$ref:prev/data/text",      # Reference to previous step
    "direct_hash_here",         # Direct hash
    "$ref:node1/data/text"      # Reference to another node
  )
)
```

### Path Extraction

When you use a path like `$ref:prev/data/text`:
- The system extracts the value at that JSON path
- Creates a new file with that value
- Uses that file hash as input

**Example JSON structure:**
```json
{
  "data": {
    "text": "Hello World",
    "count": 11
  }
}
```

**Path examples:**
- `$ref:prev/data/text` → extracts `"Hello World"`
- `$ref:prev/data/count` → extracts `11`

---

## Troubleshooting

### Common Errors and Solutions

#### Error 1: "Method expects input_a but got inputs"

**Problem**: You provided an array input (`inputs`) when the method expects named inputs (`input_a`, `input_b`).

**Solution**: Check the method definition using `get_methods(config)`. If it shows multiple entries in `file_inputs` (like `input_a`, `input_b`), use named inputs:
```r
# Wrong
file_inputs <- list(inputs = c("hash1", "hash2"))

# Correct
file_inputs <- list(
  input_a = "hash1",
  input_b = "hash2"
)
```

#### Error 2: "Array expected but got string"

**Problem**: You provided a named input (string) when the method expects an array.

**Solution**: Check the method definition. If it shows `"type": "array"`, use an array:
```r
# Wrong
file_inputs <- list(
  input_a = "hash1",
  input_b = "hash2"
)

# Correct
file_inputs <- list(
  inputs = c("hash1", "hash2")
)
```

#### Error 3: "File not found"

**Problem**: The file hash doesn't exist or the file isn't in "completed" status.

**Solution**:
- Verify files are uploaded before submitting jobs using `upload_file()`
- Check file hashes are correct (no typos)
- Ensure files have status "completed" (check using API if needed)

#### Error 4: "Reference not found" ($ref errors)

**Problem**: Referenced step/node hasn't completed or path doesn't exist.

**Solution**:
- Ensure referenced step/node completed successfully
- Verify path exists in output JSON structure (e.g., `data/text`)
- Check that dependencies are correctly specified in pipelines

#### Error 5: "Validation error" or "Missing required parameters"

**Problem**: Parameters don't match method expectations.

**Solution**:
- **Check method definition** using `get_methods(config)` - Verify parameter names match exactly
- Review method documentation for required vs optional parameters
- Ensure parameters are correct type and format

#### Error 6: "Provide either file_hash OR file_inputs, not both"

**Problem**: You provided both `file_hash` and `file_inputs`.

**Solution**: Use only one:
```r
# Wrong
query_job_by_hash(config, file_hash = "hash1", "method", list(), file_inputs = list(...))

# Correct (use file_hash)
query_job_by_hash(config, file_hash = "hash1", "method", list(), file_inputs = NULL)

# Correct (use file_inputs)
query_job_by_hash(config, file_hash = NULL, "method", list(), file_inputs = list(...))
```

### How to Verify What a Method Expects

1. **Use `get_methods()` function**:
   ```r
   methods <- get_methods(config)
   method_info <- methods$method_name
   print(method_info)
   ```

2. **Check `input_modes.file_inputs`**: This tells you:
   - Input names (e.g., `input_a`, `inputs`)
   - Input types (single file vs. array)
   - Required vs. optional

3. **Check `parameters`**: This tells you:
   - Parameter names
   - Parameter types
   - Required vs. optional

4. **Match your structure** to the method definition exactly

### Debugging Checklist

Before submitting, verify:
- [ ] Method definition checked using `get_methods(config)`
- [ ] Input names match method expectations exactly
- [ ] Named inputs use strings (single file per name)
- [ ] Array inputs use vectors (`c()`)
- [ ] `file_hash` is `NULL` when using `file_inputs`
- [ ] `file_inputs` is `NULL` when using `file_hash`
- [ ] All files are uploaded using `upload_file()`
- [ ] References (`$ref:`) point to valid steps/nodes
- [ ] Paths in references exist in output JSON structure
- [ ] Parameters match method specification
- [ ] Required parameters are provided

---

## Method Script Development: Handling Input Formats

### Important Principle: Method Scripts Must Handle Input Variations

When developing method scripts, it's crucial to understand that **your script must handle the various input formats** that the system might provide. The system delivers inputs in a standardized way, but the actual content format may vary depending on how the input was created (uploaded file, extracted from JSON, referenced from another job, etc.).

### What Your Method Script Should Handle

**Your method script** (`environment/methods/scripts/<method_name>/main.py`) should handle:

1. **Different input formats**: The system may provide inputs in various formats
   - Example: When `$ref` extracts a JSON path value (like `$ref:node1/data/mask_base64`), it creates a file containing that extracted value. If that value is base64-encoded, the file contains the base64 string as text, not the decoded binary.

2. **Method-specific processing logic**: Any logic that's specific to how your method processes data
   - Example: Detecting if an input file contains base64-encoded content and decoding it before processing

3. **Input validation**: Validating or transforming inputs in ways specific to your method
   - Example: Checking if a file is binary vs text, detecting file formats, handling edge cases

4. **Output formatting**: Formatting outputs in ways specific to your method's requirements

### Example: Handling Base64-Extracted Files

**Problem**: When using `$ref:node1/data/mask_base64` in a pipeline, the system extracts the `mask_base64` value from the JSON output and creates a file containing that base64 string as text. Your method script receives a file path, but the file contains base64 text, not the decoded binary.

**✅ Correct Approach**: Handle it in your method script
```python
# In your method script (e.g., pyradiomics/main.py)
def decode_mask_from_base64(mask_base64):
    """Decode mask from base64 string and save to temporary file."""
    mask_bytes = base64.b64decode(mask_base64)
    with tempfile.NamedTemporaryFile(suffix='.nii.gz', delete=False) as tmp_file:
        tmp_path = tmp_file.name
        tmp_file.write(mask_bytes)
    return tmp_path, None

# When reading mask file, check if it contains base64
mask_path = files['mask']  # From metadata
# Check if file contains base64-encoded content
try:
    with open(mask_path, 'rb') as f:
        first_bytes = f.read(100)
        # Check if file starts with base64-like content (text, not binary)
        if len(first_bytes) > 0 and first_bytes[0] not in [0x00, 0x1E, 0x5C]:
            # Try to read as text and decode base64
            with open(mask_path, 'r', encoding='utf-8') as text_file:
                content = text_file.read().strip()
                if all(c.isalnum() or c in '+/=' for c in content) and len(content) > 100:
                    # Likely base64, decode it
                    mask_bytes = base64.b64decode(content)
                    # Create temporary file with decoded content
                    with tempfile.NamedTemporaryFile(suffix='.nii.gz', delete=False) as tmp_file:
                        tmp_path = tmp_file.name
                        tmp_file.write(mask_bytes)
                    mask_path = tmp_path
except Exception:
    # If reading fails, continue with original file path
    pass
```

### Key Takeaways

1. **Method scripts handle input variations**: Each method script should handle the various formats it might receive
2. **Detect and adapt**: Method scripts should detect input formats and adapt accordingly
3. **Error handling**: Method scripts should gracefully handle unexpected input formats
4. **Don't assume input format**: Always validate and handle different possible input formats

### Best Practices for Method Scripts

1. **Always validate inputs**: Check file existence, format, and content before processing
2. **Handle multiple formats**: Be prepared to receive inputs in different formats (binary, text, base64, etc.)
3. **Provide clear errors**: If an input format is unsupported, provide a clear error message
4. **Clean up temporary files**: If you create temporary files, clean them up after use
5. **Document assumptions**: Document what input formats your method expects and handles

---

## Array Ordering and Deduplication

### Important: Arrays Preserve Order

**Arrays in `file_inputs` are NOT automatically sorted** - their order is preserved exactly as you provide it. This is intentional because the order of array elements may be semantically important for your use case.

### What Gets Sorted (For Deduplication)

The system automatically sorts certain elements for **deduplication purposes** (to ensure that identical jobs/meta-jobs/pipelines are detected correctly):

1. **Parameter keys**: Parameters are sorted alphabetically by key name (recursively for nested structures)
2. **File input keys**: The names of `file_inputs` are sorted alphabetically (e.g., `input_a`, `input_b` → sorted alphabetically)
3. **Node IDs**: Pipeline node IDs are sorted alphabetically

### What Does NOT Get Sorted

**Array elements within `file_inputs` are NOT sorted** - their order is preserved exactly as provided:

```r
# This array order is PRESERVED
file_inputs <- list(
  inputs = c("hash3", "hash1", "hash2")  # Order: hash3, hash1, hash2
)

# The system will use this exact order for deduplication
# If you want sorted order, you must sort it yourself:
file_inputs <- list(
  inputs = sort(c("hash3", "hash1", "hash2"))  # Now: hash1, hash2, hash3
)
```

### Why Sorting Matters (Deduplication)

The sorting of keys (but not array elements) ensures **deterministic hash computation** for deduplication:

- **Same parameters in different order** → Same hash (because parameters are sorted)
- **Same file_inputs keys in different order** → Same hash (because keys are sorted)
- **Same array elements in different order** → **Different hash** (because arrays preserve order)

### When to Sort Arrays Yourself

**You should sort arrays yourself** if:

1. **Order doesn't matter**: If the order of elements in your array doesn't affect the result, sort them to improve deduplication
   ```r
   # If order doesn't matter, sort for better deduplication
   file_inputs <- list(
     inputs = sort(c("hash3", "hash1", "hash2"))
   )
   ```

2. **You want consistent deduplication**: If you want `c("hash1", "hash2")` and `c("hash2", "hash1")` to be treated as the same job, sort them

**You should NOT sort arrays** if:

1. **Order matters**: If the order of elements affects your method's output, preserve the order
   ```r
   # If order matters (e.g., processing sequence), preserve it
   file_inputs <- list(
     inputs = c("hash3", "hash1", "hash2")  # Order preserved
   )
   ```

2. **Semantic meaning**: If the array order has semantic meaning (e.g., time series, processing order), preserve it

### Example: When Order Matters vs When It Doesn't

**Order matters** (e.g., time series data):
```r
# Preserve order - each file represents a time point
file_inputs <- list(
  time_series = c("t1_hash", "t2_hash", "t3_hash")  # Order is critical!
)
```

**Order doesn't matter** (e.g., batch processing):
```r
# Sort for better deduplication - order doesn't affect result
file_inputs <- list(
  batch_files = sort(c("file3_hash", "file1_hash", "file2_hash"))
)
```

### Summary

- **Arrays preserve order**: The system does NOT auto-sort array elements
- **Keys are sorted**: Parameter keys and file_input keys are sorted for deduplication
- **Developer decides**: You decide whether to sort arrays based on whether order matters
- **Use `sort()` function**: If you want arrays sorted, use R's `sort()` function before submitting

## Best Practices

### 1. Always Check Method Definition First

**Before writing any code**, check what the method expects:

```r
methods <- get_methods(config)
method_info <- methods$method_name

# Check inputs
print(method_info$input_modes)

# Check parameters
print(method_info$parameters)
```

### 2. Use Consistent Naming

Use descriptive variable names that match method input names:

```r
# Good: Matches method definition
file_inputs <- list(
  input_a = file_a_hash,
  input_b = file_b_hash
)

# Avoid: Unclear naming
file_inputs <- list(
  input_a = hash1,
  input_b = hash2
)
```

### 3. Upload Files Before Submitting

Always upload files first and verify they exist:

```r
# Upload file
file_hash <- upload_file(config, content = charToRaw(content), filename = "data.json")

# Verify hash is correct
cat(sprintf("File hash: %s\n", file_hash))

# Then use in job submission
result <- query_job_by_hash(config, file_hash = file_hash, "method", list())
```

### 4. Use Appropriate Timeouts

```r
# Simple jobs: 60 seconds
result <- wait_for_job_results_by_hash(..., timeout = 60)

# Meta-jobs: 120-300 seconds
result <- wait_for_meta_job_results(..., timeout = 120)

# Complex pipelines: 300+ seconds
result <- wait_for_pipeline(..., timeout = 300)
```

### 5. Handle Errors Gracefully

```r
result <- tryCatch({
  query_job_by_hash(config, file_hash = NULL, "method_name", list(), file_inputs = file_inputs)
}, error = function(e) {
  cat(sprintf("Error: %s\n", e$message))
  # Log error details
  return(NULL)
})
```

### 6. Use Specific Paths in References

Prefer specific paths over complete outputs when possible:

```r
# Good: Specific path
file_inputs = list(input = "$ref:prev/data/text")

# Less ideal: Complete output (larger, may contain unnecessary data)
file_inputs = list(input = "$ref:prev")
```

### 7. Validate Parameters

Use parameter validation to catch errors early:

```r
result <- query_job_by_hash(
  config,
  file_hash = NULL,
  method_name = "some_method",
  parameters = list(count = 100),
  validate_parameters = TRUE  # Validates before submission
)
```

### 8. Visual Checklist Before Submitting

**Before submitting any job/meta-job/pipeline, verify:**

- [ ] **Method definition checked**: Used `get_methods(config)` to know what inputs/parameters are expected
- [ ] **Input names match**: Your `file_inputs` keys match method `file_inputs` names exactly
- [ ] **Input types match**: Named inputs use strings, arrays use vectors
- [ ] **file_hash is NULL**: When using `file_inputs`, set `file_hash = NULL`
- [ ] **file_inputs is NULL**: When using `file_hash`, set `file_inputs = NULL`
- [ ] **Files uploaded**: All file hashes exist (uploaded using `upload_file()`)
- [ ] **References valid**: `$ref:` references point to valid steps/nodes
- [ ] **Paths exist**: If using path extraction (`$ref:prev/data/text`), verify the path exists
- [ ] **Parameters correct**: Parameters match method expectations (types, formats, required vs optional)
- [ ] **Dependencies correct**: Pipeline nodes have correct dependencies

### 9. Test with Simple Examples First

Start with simple examples, then build complexity:

1. **Test single job** with single file
2. **Test single job** with named inputs
3. **Test single job** with array input
4. **Test meta-job** with references
5. **Test pipeline** with multiple nodes

This helps identify issues early and understand how the system works.

---

## Function Reference

### Jobs

#### `query_job(config, content, method_name, parameters, validate_parameters)`
Submit a job with content directly (dsHPC calculates hash automatically).

**Parameters:**
- `config`: API configuration (from `create_api_config()`)
- `content`: Content to process (raw vector, character, or R object)
- `method_name`: Name of the method to execute
- `parameters`: Named list of parameters (default: `list()`)
- `validate_parameters`: Whether to validate parameters (default: `TRUE`)

**Returns:** Job response with status and hash

#### `query_job_by_hash(config, file_hash, method_name, parameters, file_inputs, validate_parameters)`
Submit a job using a file hash or file_inputs.

**Parameters:**
- `config`: API configuration
- `file_hash`: Hash of the content, or `NULL` for params-only or multi-file jobs
- `method_name`: Name of the method to execute
- `parameters`: Named list of parameters (default: `list()`)
- `file_inputs`: Named list of file hashes for multi-file jobs, or `NULL` (default: `NULL`)
- `validate_parameters`: Whether to validate parameters (default: `TRUE`)

**Returns:** Job response with status and hash

#### `wait_for_job_results(config, content, method_name, parameters, timeout, interval, parse_json, validate_parameters)`
Submit a job and wait for results in one call (using content).

**Parameters:**
- `config`: API configuration
- `content`: Content to process
- `method_name`: Name of the method
- `parameters`: Named list of parameters (default: `list()`)
- `timeout`: Maximum time to wait in seconds (default: `NA` for no timeout)
- `interval`: Polling interval in seconds (default: `5`)
- `parse_json`: Whether to parse output as JSON (default: `TRUE`)
- `validate_parameters`: Whether to validate parameters (default: `TRUE`)

**Returns:** Job output (parsed JSON if `parse_json = TRUE`)

#### `wait_for_job_results_by_hash(config, file_hash, method_name, parameters, timeout, interval, parse_json, validate_parameters, file_inputs)`
Submit a job and wait for results in one call (using hash or file_inputs).

**Parameters:**
- `config`: API configuration
- `file_hash`: Hash of the content, or `NULL`
- `method_name`: Name of the method
- `parameters`: Named list of parameters (default: `list()`)
- `timeout`: Maximum time to wait in seconds (default: `NA`)
- `interval`: Polling interval in seconds (default: `5`)
- `parse_json`: Whether to parse output as JSON (default: `TRUE`)
- `validate_parameters`: Whether to validate parameters (default: `TRUE`)
- `file_inputs`: Named list of file hashes, or `NULL` (default: `NULL`)

**Returns:** Job output (parsed JSON if `parse_json = TRUE`)

#### `get_job_status_by_hash(config, file_hash, method_name, parameters, file_inputs)`
Get the status of a job.

**Parameters:**
- `config`: API configuration
- `file_hash`: Hash of the content, or `NULL`
- `method_name`: Name of the method
- `parameters`: Named list of parameters (default: `list()`)
- `file_inputs`: Named list of file hashes, or `NULL` (default: `NULL`)

**Returns:** Job status string

#### `job_succeeded_by_hash(config, file_hash, method_name, parameters, file_inputs)`
Check if a job succeeded.

**Parameters:** Same as `get_job_status_by_hash()`

**Returns:** `TRUE` if succeeded, `FALSE` otherwise

#### `get_job_output_by_hash(config, file_hash, method_name, parameters, parse_json, file_inputs)`
Get the output of a completed job.

**Parameters:**
- `config`: API configuration
- `file_hash`: Hash of the content, or `NULL`
- `method_name`: Name of the method
- `parameters`: Named list of parameters (default: `list()`)
- `parse_json`: Whether to parse output as JSON (default: `TRUE`)
- `file_inputs`: Named list of file hashes, or `NULL` (default: `NULL`)

**Returns:** Job output (parsed JSON if `parse_json = TRUE`)

### Meta-Jobs

#### `submit_meta_job(config, content, method_chain)`
Submit a meta-job with content directly.

**Parameters:**
- `config`: API configuration
- `content`: Initial content to process
- `method_chain`: List of method specifications

**Returns:** Meta-job response with `meta_job_hash`

#### `submit_meta_job_by_hash(config, file_hash, method_chain)`
Submit a meta-job using a file hash.

**Parameters:**
- `config`: API configuration
- `file_hash`: Hash of the initial content, or `NULL` for params-only
- `method_chain`: List of method specifications

**Returns:** Meta-job response with `meta_job_hash`

#### `submit_meta_job_multi(config, file_inputs, method_chain)`
Submit a meta-job with multiple input files.

**Parameters:**
- `config`: API configuration
- `file_inputs`: Named list of file hashes (supports named inputs and arrays)
- `method_chain`: List of method specifications

**Returns:** Meta-job response with `meta_job_hash`

#### `wait_for_meta_job_results(config, meta_job_hash, timeout, interval, parse_json)`
Wait for a meta-job to complete and return results.

**Parameters:**
- `config`: API configuration
- `meta_job_hash`: ID of the meta-job
- `timeout`: Maximum time to wait in seconds (default: `NA`)
- `interval`: Polling interval in seconds (default: `5`)
- `parse_json`: Whether to parse output as JSON (default: `TRUE`)

**Returns:** Final output of the meta-job chain

#### `get_meta_job_status(config, meta_job_hash)`
Get the status of a meta-job.

**Parameters:**
- `config`: API configuration
- `meta_job_hash`: ID of the meta-job

**Returns:** Meta-job status information

#### `execute_processing_chain(config, content, method_chain, timeout, interval, parse_json)`
Submit a meta-job and wait for results in one call (using content).

**Parameters:** Same as `submit_meta_job()` + `wait_for_meta_job_results()`

**Returns:** Final output of the chain

#### `execute_processing_chain_by_hash(config, file_hash, method_chain, timeout, interval, parse_json)`
Submit a meta-job and wait for results in one call (using hash).

**Parameters:** Same as `submit_meta_job_by_hash()` + `wait_for_meta_job_results()`

**Returns:** Final output of the chain

#### `execute_processing_chain_multi(config, file_inputs, method_chain, timeout, interval, parse_json)`
Submit a meta-job and wait for results in one call (using file_inputs).

**Parameters:** Same as `submit_meta_job_multi()` + `wait_for_meta_job_results()`

**Returns:** Final output of the chain

### Pipelines

#### `submit_pipeline(config, pipeline_definition)`
Submit a pipeline.

**Parameters:**
- `config`: API configuration
- `pipeline_definition`: List with `nodes` (created using `create_pipeline_node()`)

**Returns:** Pipeline response with `pipeline_hash`

#### `wait_for_pipeline(config, pipeline_hash, timeout, interval, verbose, parse_json)`
Wait for a pipeline to complete and return output.

**Parameters:**
- `config`: API configuration
- `pipeline_hash`: Pipeline ID
- `timeout`: Maximum time to wait in seconds (default: `NA`)
- `interval`: Polling interval in seconds (default: `5`)
- `verbose`: Show progress updates (default: `TRUE`)
- `parse_json`: Whether to parse output as JSON (default: `TRUE`)

**Returns:** Final pipeline output

#### `get_pipeline_status(config, pipeline_hash)`
Get the status of a pipeline.

**Parameters:**
- `config`: API configuration
- `pipeline_hash`: Pipeline ID

**Returns:** Pipeline status information

#### `execute_pipeline(config, pipeline_definition, timeout, interval, verbose)`
Submit a pipeline and wait for results in one call.

**Parameters:** Same as `submit_pipeline()` + `wait_for_pipeline()`

**Returns:** Final pipeline output

#### `create_pipeline_node(chain, dependencies, input_file_hash)`
Create a pipeline node.

**Parameters:**
- `chain`: List of method specifications for this node
- `dependencies`: Character vector of node IDs this node depends on (default: `character(0)`)
- `input_file_hash`: Legacy single file hash (default: `NULL`, use `file_inputs` in chain instead)

**Returns:** Pipeline node structure

### Files

#### `upload_file(config, content, filename, chunk_size_mb, show_progress)`
Upload a file to the system.

**Parameters:**
- `config`: API configuration
- `content`: File content (raw vector)
- `filename`: Name of the file
- `chunk_size_mb`: Chunk size for large files in MB (default: `10`)
- `show_progress`: Show upload progress (default: `TRUE`)

**Returns:** File hash

#### `upload_object(config, obj, filename, chunk_size_mb, show_progress)`
Upload an R object (serializes to JSON automatically).

**Parameters:** Same as `upload_file()`, but `obj` is an R object instead of raw content

**Returns:** File hash

#### `hash_content(content)`
Calculate hash of content (for deduplication).

**Parameters:**
- `content`: Content to hash (raw vector, character, or R object)

**Returns:** Content hash string

### Utilities

#### `get_methods(config)`
Get all available methods and their specifications.

**Parameters:**
- `config`: API configuration

**Returns:** Named list of method specifications

#### `validate_method_parameters(method_name, params, method_spec)`
Validate parameters against method specification.

**Parameters:**
- `method_name`: Name of the method
- `params`: Named list of parameters to validate
- `method_spec`: Method specification (from `get_methods()`)

**Returns:** `TRUE` if valid, throws error if invalid

#### `create_api_config(base_url, port, api_key, auth_header, timeout)`
Create API configuration.

**Parameters:**
- `base_url`: Base URL of the API (e.g., `"http://localhost"`)
- `port`: Port number (e.g., `8001`)
- `api_key`: API key for authentication
- `auth_header`: Authorization header name (default: `"Authorization"`)
- `timeout`: Request timeout in seconds (default: `30`)

**Returns:** API configuration object

---

## Important Notes

1. **Exactly one of `file_hash` or `file_inputs`**: You cannot use both simultaneously. Use `NULL` for the one you're not using.

2. **Both can be NULL**: For params-only jobs (jobs that only use parameters, no files).

3. **Arrays preserve order**: The order of elements in an array is important and preserved exactly as you specify.

4. **Automatic deduplication**: The system automatically detects identical jobs/meta-jobs/pipelines and reuses existing results.

5. **References resolved automatically**: `$ref:` references are automatically resolved before executing each step.

6. **Pipelines require single terminal node**: All pipelines must converge to exactly one final node.

7. **File status matters**: Files must be uploaded and in "completed" status before they can be used as inputs.

8. **Always check method definition first**: Before submitting, use `get_methods(config)` to know what inputs/parameters are expected.

9. **Use dsHPC package functions**: All examples in this guide use functions from the `dsHPC` package. Do not use internal API functions directly.

---

**Remember**: The key to success is understanding what your method expects and matching your `file_inputs` structure exactly. Always check the method definition using `get_methods(config)` first!
