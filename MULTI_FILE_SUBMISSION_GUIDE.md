# Complete Guide: Submitting Jobs, Meta-Jobs, and Pipelines with Multi-File Support

This comprehensive guide explains how to use the dsHPC system to submit jobs, meta-jobs, and pipelines with support for multiple input files, including file arrays and references to previous outputs.

## Table of Contents

1. [Basic Concepts](#basic-concepts)
2. [Normal Jobs](#normal-jobs)
3. [Meta-Jobs](#meta-jobs)
4. [Pipelines](#pipelines)
5. [Reference System ($ref)](#reference-system-ref)
6. [Complete Examples](#complete-examples)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

---

## Basic Concepts

### Input Types

The dsHPC system supports three ways to specify inputs:

1. **Single File (Legacy)**: One file using `file_hash`
2. **Multi-File**: Multiple named files using `file_inputs` (dictionary)
3. **Arrays**: Arrays of files within `file_inputs` (for methods that accept multiple files of the same type)

### Understanding `file_inputs`

The `file_inputs` parameter is a **named dictionary** where:
- **Keys** are input names (e.g., `input_a`, `inputs`, `primary`)
- **Values** can be:
  - A single file hash (string)
  - An array of file hashes (vector/list)

**Examples:**

```r
# Single file per input name
file_inputs <- list(
  input_a = "hash1",
  input_b = "hash2"
)

# Array of files for one input
file_inputs <- list(
  inputs = c("hash1", "hash2", "hash3")  # Array of hashes
)

# Mixed: some single files, some arrays
file_inputs <- list(
  primary = "hash1",
  references = c("hash2", "hash3", "hash4")
)
```

### Important Rules

1. **Exactly one of `file_hash` OR `file_inputs`**: You cannot use both at the same time. Use `NULL` for the one you're not using.
2. **Both can be NULL**: For params-only jobs (jobs that only use parameters, no files).
3. **Arrays preserve order**: The order of elements in an array is important and preserved exactly as you specify.
4. **Automatic deduplication**: The system automatically detects identical jobs/meta-jobs/pipelines and reuses existing results.

---

## Normal Jobs

### 1. Single File (Legacy Method)

**When to use**: When you have one input file and want to use the traditional `file_hash` approach.

**Step-by-step:**

```r
library(dsHPC)

# 1. Create API configuration
config <- create_api_config("http://localhost", 8001, "your_api_key")

# 2. Prepare your file content
file_content <- jsonlite::toJSON(list(data = list(text = "Hello")), auto_unbox = TRUE)

# 3. Upload file and get hash
file_hash <- upload_file(config, content = charToRaw(file_content), filename = "input.json")

# 4. Submit job with single file
result <- query_job_by_hash(
  config,
  file_hash = file_hash,        # Single file hash
  method_name = "concat",
  parameters = list(),
  file_inputs = NULL             # Must be NULL when using file_hash
)

# 5. Wait for results
output <- wait_for_job_results_by_hash(
  config,
  file_hash = file_hash,
  method_name = "concat",
  parameters = list(),
  timeout = 60,
  parse_json = TRUE
)

# 6. Extract and use result
output_text <- output$data$text
cat(sprintf("Result: %s\n", output_text))
```

### 2. Multi-File Inputs

**When to use**: When your method requires multiple named input files (e.g., `input_a` and `input_b`).

**Step-by-step:**

```r
# 1. Upload multiple files
file1_content <- jsonlite::toJSON(list(data = list(text = "Hello")), auto_unbox = TRUE)
file2_content <- jsonlite::toJSON(list(data = list(text = "World")), auto_unbox = TRUE)

file1_hash <- upload_file(config, content = charToRaw(file1_content), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(file2_content), filename = "file2.json")

# 2. Define file_inputs as a named list
# IMPORTANT: The names must match what the method expects (check method documentation)
file_inputs <- list(
  input_a = file1_hash,  # Named input
  input_b = file2_hash   # Named input
)

# 3. Submit job with multi-file inputs
result <- query_job_by_hash(
  config,
  file_hash = NULL,              # Must be NULL when using file_inputs
  method_name = "concat",
  parameters = list(),
  file_inputs = file_inputs      # Named list of file hashes
)

# 4. Wait for results (use same file_inputs)
output <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,              # NULL when using file_inputs
  method_name = "concat",
  parameters = list(),
  file_inputs = file_inputs,     # Same file_inputs as submission
  timeout = 60,
  parse_json = TRUE
)

# 5. Extract result
output_text <- output$data$text
# Expected: "HelloWorld"
```

**Key Points:**
- Use `file_hash = NULL` when using `file_inputs`
- The input names (`input_a`, `input_b`) must match what the method expects
- Use the same `file_inputs` when waiting for results

### 3. Arrays in `file_inputs`

**When to use**: When your method accepts an array of files for one input (e.g., `concat_multi` with `inputs` array).

**Step-by-step:**

```r
# 1. Upload multiple files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "!")))), filename = "file3.json")

# 2. Define array of files
# IMPORTANT: Use c() to create a vector/array
file_inputs <- list(
  inputs = c(file1_hash, file2_hash, file3_hash)  # Array of hashes
)

# 3. Submit job with array
result <- query_job_by_hash(
  config,
  file_hash = NULL,
  method_name = "concat_multi",
  parameters = list(separator = "|"),  # Method-specific parameter
  file_inputs = file_inputs
)

# 4. Wait for results
output <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,
  method_name = "concat_multi",
  parameters = list(separator = "|"),
  file_inputs = file_inputs,
  timeout = 60,
  parse_json = TRUE
)

# 5. Extract result
output_text <- output$data$text
# Expected: "Hello|World|!"
```

**Key Points:**
- Arrays are created using `c()` (combine function)
- The array order is preserved exactly as specified
- Use the same array when waiting for results

### 4. Params-Only Jobs (No Files)

**When to use**: When your method only needs parameters and no input files.

```r
# Submit job with only parameters
result <- query_job_by_hash(
  config,
  file_hash = NULL,              # NULL
  method_name = "some_method",
  parameters = list(param1 = "value1", param2 = "value2"),
  file_inputs = NULL              # NULL also
)

# Wait for results
output <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,
  method_name = "some_method",
  parameters = list(param1 = "value1", param2 = "value2"),
  file_inputs = NULL,
  timeout = 60
)
```

---

## Meta-Jobs

Meta-jobs allow you to chain multiple methods sequentially, where the output of one step becomes the input of the next.

### Understanding Meta-Jobs

- **Initial Input**: Can be `initial_file_hash` (single file) or `initial_file_inputs` (multi-file)
- **Chain**: List of method steps executed sequentially
- **References**: Use `$ref:prev` to reference the output of the previous step

### 1. Single File Meta-Job

**Step-by-step:**

```r
# 1. Upload initial file
file_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "input.json")

# 2. Define method chain
# Each step is a list with method_name, parameters, and optionally file_inputs
chain <- list(
  # Step 1: Process initial file
  list(
    method_name = "concat",
    parameters = list(a = "Hello", b = "World")
  ),
  # Step 2: Use output from step 1 using $ref:prev
  list(
    method_name = "append_line",
    parameters = list(line = "Extra"),
    file_inputs = list(input = "$ref:prev")  # Reference to previous step output
  )
)

# 3. Submit meta-job
meta_response <- submit_meta_job_by_hash(config, file_hash = file_hash, method_chain = chain)

# 4. Wait for results
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)

# 5. Extract final output
output_text <- result$data$text
```

### 2. Multi-File Meta-Job with `initial_file_inputs`

**Step-by-step:**

```r
# 1. Upload multiple files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")

# 2. Define initial_file_inputs
file_inputs <- list(
  input_a = file1_hash,
  input_b = file2_hash
)

# 3. Define chain (first step uses initial_file_inputs automatically)
chain <- list(
  list(method_name = "concat", parameters = list())
)

# 4. Submit meta-job with multi-file
meta_response <- submit_meta_job_multi(config, file_inputs, chain)

# 5. Wait for results
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)

# 6. Extract result
output_text <- result$data$text
# Expected: "HelloWorld"
```

**Key Points:**
- Use `submit_meta_job_multi()` for multi-file inputs
- The first step automatically receives `initial_file_inputs`
- Subsequent steps can use `$ref:prev` to reference previous outputs

### 3. Arrays in `initial_file_inputs`

**Step-by-step:**

```r
# 1. Upload multiple files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "!")))), filename = "file3.json")

# 2. Define array in initial_file_inputs
file_inputs <- list(
  inputs = c(file1_hash, file2_hash, file3_hash)  # Array
)

# 3. Define chain
chain <- list(
  list(method_name = "concat_multi", parameters = list(separator = "|"))
)

# 4. Submit meta-job
meta_response <- submit_meta_job_multi(config, file_inputs, chain)

# 5. Wait for results
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)

# 6. Extract result
output_text <- result$data$text
# Expected: "Hello|World|!"
```

### 4. `file_inputs` in Intermediate Steps

**When to use**: When you need to provide additional files in a step other than the first one.

**Step-by-step:**

```r
# 1. Upload files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "!")))), filename = "file3.json")

# 2. Define initial_file_inputs for first step
initial_file_inputs <- list(
  input_a = file1_hash,
  input_b = file2_hash
)

# 3. Define chain with file_inputs in intermediate steps
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

# 4. Submit meta-job
meta_response <- submit_meta_job_multi(config, initial_file_inputs, chain)

# 5. Wait for results
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)

# 6. Extract result
output_text <- result$data$text
# Expected: "Hello|World|!" (from second step)
```

**Key Points:**
- First step uses `initial_file_inputs` automatically
- Intermediate steps can have their own `file_inputs`
- You can mix `$ref:prev` references and direct file hashes

---

## Pipelines

Pipelines allow you to create DAGs (Directed Acyclic Graphs) of meta-jobs, where multiple nodes can execute in parallel and their outputs can be combined.

### Understanding Pipelines

- **Nodes**: Each node is a meta-job (chain of methods)
- **Dependencies**: Nodes can depend on other nodes
- **Parallel Execution**: Nodes without dependencies run in parallel
- **Convergence**: Multiple nodes can converge into one final node
- **References**: Use `$ref:nodeX` to reference outputs from other nodes

### 1. Simple Pipeline with `file_inputs`

**Step-by-step:**

```r
# 1. Upload files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")

# 2. Create pipeline node with file_inputs
node1 <- create_pipeline_node(
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
  dependencies = character(0)  # No dependencies (root node)
)

# 3. Create pipeline
pipeline <- list(
  nodes = list(
    node1 = node1
  )
)

# 4. Submit pipeline
pipeline_response <- submit_pipeline(config, pipeline)

# 5. Wait for results
result <- wait_for_pipeline(config, pipeline_response$pipeline_hash, timeout = 120, parse_json = TRUE)

# 6. Extract final output
output_text <- result$final_output$data$text
# Expected: "HelloWorld"
```

### 2. Pipeline with Arrays in `file_inputs`

**Step-by-step:**

```r
# 1. Upload files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Hello")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "World")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "!")))), filename = "file3.json")

# 2. Create node with array in file_inputs
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

# 3. Create and submit pipeline
pipeline <- list(nodes = list(node1 = node1))
pipeline_response <- submit_pipeline(config, pipeline)
result <- wait_for_pipeline(config, pipeline_response$pipeline_hash, timeout = 120, parse_json = TRUE)

# 4. Extract result
output_text <- result$final_output$data$text
# Expected: "Hello|World|!"
```

### 3. Complex Pipeline with Multiple Parallel Nodes Converging

**Step-by-step:**

```r
# 1. Upload initial files
file1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "A")))), filename = "file1.json")
file2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "B")))), filename = "file2.json")
file3_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "C")))), filename = "file3.json")
file4_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "D")))), filename = "file4.json")

# 2. Node A: Process file1 and file2 (runs in parallel with B and C)
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
  dependencies = character(0)  # No dependencies = root node
)

# 3. Node B: Process file3 only (runs in parallel with A and C)
node_b <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat",
      parameters = list(a = "C", b = "D")
    )
  ),
  dependencies = character(0)  # No dependencies = root node
)

# 4. Node C: Process file3 and file4 (runs in parallel with A and B)
node_c <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat",
      parameters = list(),
      file_inputs = list(
        input_a = file3_hash,
        input_b = file4_hash
      )
    )
  ),
  dependencies = character(0)  # No dependencies = root node
)

# 5. Node Final: Combine outputs from A, B, and C using $ref
node_final <- create_pipeline_node(
  chain = list(
    list(
      method_name = "concat_multi",
      parameters = list(separator = "|"),
      file_inputs = list(
        inputs = list(
          "$ref:node_a/data/text",  # Reference to node_a output
          "$ref:node_b/data/text",   # Reference to node_b output
          "$ref:node_c/data/text"    # Reference to node_c output
        )
      )
    )
  ),
  dependencies = c("node_a", "node_b", "node_c")  # Depends on all three
)

# 6. Create pipeline
pipeline <- list(
  nodes = list(
    node_a = node_a,
    node_b = node_b,
    node_c = node_c,
    node_final = node_final
  )
)

# 7. Submit pipeline
pipeline_response <- submit_pipeline(config, pipeline)

# 8. Wait for results
result <- wait_for_pipeline(config, pipeline_response$pipeline_hash, timeout = 180, parse_json = TRUE)

# 9. Extract final output
output_text <- result$final_output$data$text
# Expected: "AB|CD|CD" (or similar, depending on your inputs)
```

**Key Points:**
- Nodes without dependencies run in parallel
- Use `dependencies` to specify execution order
- Use `$ref:nodeX` to reference outputs from other nodes
- The final node combines all parallel results

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

## Complete Examples

### Example 1: Image Processing with Multiple References

```r
library(dsHPC)

config <- create_api_config("http://localhost", 8001, "your_api_key")

# 1. Upload main image
image_hash <- upload_file(
  config,
  content = readBin("image.jpg", "raw", file.info("image.jpg")$size),
  filename = "image.jpg"
)

# 2. Upload reference mask
mask_hash <- upload_file(
  config,
  content = readBin("mask.jpg", "raw", file.info("mask.jpg")$size),
  filename = "mask.jpg"
)

# 3. Meta-job: Apply mask then extract features
initial_inputs <- list(
  image = image_hash,
  mask = mask_hash
)

chain <- list(
  # Step 1: Apply mask using both files
  list(
    method_name = "apply_mask",
    parameters = list(),
    file_inputs = list(
      image = image_hash,
      mask = mask_hash
    )
  ),
  # Step 2: Extract features from result
  list(
    method_name = "extract_features",
    parameters = list(feature_set = "all"),
    file_inputs = list(
      input = "$ref:prev"  # Use complete output from step 1
    )
  )
)

# 4. Submit with initial_file_inputs
meta_response <- submit_meta_job_multi(config, initial_inputs, chain)

# 5. Wait for results
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 300, parse_json = TRUE)

# 6. Use result
features <- result$data$features
```

### Example 2: Analysis Pipeline with Parallelization

```r
library(dsHPC)

config <- create_api_config("http://localhost", 8001, "your_api_key")

# 1. Upload multiple datasets
upload_json <- function(text) {
  content <- jsonlite::toJSON(list(data = list(text = text)), auto_unbox = TRUE)
  upload_file(config, content = charToRaw(content), filename = "data.json")
}

dataset1_hash <- upload_json("Dataset1")
dataset2_hash <- upload_json("Dataset2")
dataset3_hash <- upload_json("Dataset3")

# 2. Node 1: Analyze dataset1
node1 <- create_pipeline_node(
  chain = list(
    list(
      method_name = "analyze",
      parameters = list(analysis_type = "statistical"),
      file_inputs = list(input = dataset1_hash)
    )
  ),
  dependencies = character(0)
)

# 3. Node 2: Analyze dataset2 (parallel with node1)
node2 <- create_pipeline_node(
  chain = list(
    list(
      method_name = "analyze",
      parameters = list(analysis_type = "statistical"),
      file_inputs = list(input = dataset2_hash)
    )
  ),
  dependencies = character(0)
)

# 4. Node 3: Analyze dataset3 (parallel with node1 and node2)
node3 <- create_pipeline_node(
  chain = list(
    list(
      method_name = "analyze",
      parameters = list(analysis_type = "statistical"),
      file_inputs = list(input = dataset3_hash)
    )
  ),
  dependencies = character(0)
)

# 5. Final node: Combine all results
node_final <- create_pipeline_node(
  chain = list(
    list(
      method_name = "merge_results",
      parameters = list(merge_strategy = "concatenate"),
      file_inputs = list(
        results = list(
          "$ref:node1/data/results",
          "$ref:node2/data/results",
          "$ref:node3/data/results"
        )
      )
    )
  ),
  dependencies = c("node1", "node2", "node3")
)

# 6. Create and submit pipeline
pipeline <- list(
  nodes = list(
    node1 = node1,
    node2 = node2,
    node3 = node3,
    node_final = node_final
  )
)

pipeline_response <- submit_pipeline(config, pipeline)
result <- wait_for_pipeline(config, pipeline_response$pipeline_hash, timeout = 600, parse_json = TRUE)

# 7. Use combined results
combined_results <- result$final_output$data$results
```

### Example 3: Processing with Arrays and Mixed References

```r
library(dsHPC)

config <- create_api_config("http://localhost", 8001, "your_api_key")

# 1. Upload base files
base1_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Base1")))), filename = "base1.json")
base2_hash <- upload_file(config, content = charToRaw(jsonlite::toJSON(list(data = list(text = "Base2")))), filename = "base2.json")

# 2. Meta-job with initial array and references in intermediate steps
initial_inputs <- list(
  bases = c(base1_hash, base2_hash)  # Initial array
)

chain <- list(
  # Step 1: Process initial array
  list(
    method_name = "process_array",
    parameters = list(mode = "parallel")
  ),
  # Step 2: Use output from step 1 and add more files
  list(
    method_name = "combine_results",
    parameters = list(separator = "|"),
    file_inputs = list(
      inputs = list(
        "$ref:prev/data/results",  # Output from previous step
        base1_hash,                 # Direct hash
        base2_hash                  # Direct hash
      )
    )
  )
)

meta_response <- submit_meta_job_multi(config, initial_inputs, chain)
result <- wait_for_meta_job_results(config, meta_response$meta_job_hash, timeout = 120, parse_json = TRUE)
```

---

## Best Practices

### 1. Sorting and Consistency

- **Always use sorting functions**: The system automatically sorts `file_inputs`, `parameters`, and `dependencies` to ensure deterministic hashing.
- **Sort arrays manually if needed**: Although the system preserves array order, ensure consistent ordering if you need deduplication.

### 2. File Validation

```r
# Before submitting, verify files exist and are ready
check_file_exists <- function(config, file_hash) {
  # Check file status using API
  # Return TRUE if file exists and is completed
}

# Validate all files before submitting
for (hash in c(file1_hash, file2_hash, file3_hash)) {
  if (!check_file_exists(config, hash)) {
    stop(sprintf("File with hash %s does not exist or is not ready", hash))
  }
}
```

### 3. Error Handling

```r
# Use tryCatch to handle errors gracefully
result <- tryCatch({
  query_job_by_hash(config, file_hash = NULL, "method_name", list(), file_inputs = file_inputs)
}, error = function(e) {
  cat(sprintf("Error: %s\n", e$message))
  return(NULL)
})
```

### 4. Timeouts and Polling

```r
# Use appropriate timeouts based on job size
# Simple jobs: 60 seconds
# Meta-jobs: 300 seconds
# Complex pipelines: 600+ seconds

result <- wait_for_job_results_by_hash(
  config,
  file_hash = NULL,
  method_name = "method",
  parameters = list(),
  file_inputs = file_inputs,
  timeout = 60,      # Timeout in seconds
  interval = 5       # Polling interval in seconds
)
```

### 5. JSON Data Structure

```r
# Use consistent structure for JSON files
standard_json <- list(
  data = list(
    text = "your_text_here",
    # other fields...
  )
)

# Serialize correctly
json_content <- jsonlite::toJSON(standard_json, auto_unbox = TRUE)
file_hash <- upload_file(config, content = charToRaw(json_content), filename = "data.json")
```

### 6. Reference ($ref) Usage

- **Use specific paths when possible**: `$ref:prev/data/text` is more specific than `$ref:prev`
- **Verify paths exist**: Ensure the output structure has the paths you reference
- **Don't use $ref:prev in first step**: The first step has no previous step

### 7. Debugging

```r
# Check status before waiting for results
status <- get_job_status_by_hash(config, file_hash = NULL, "method_name", list(), file_inputs = file_inputs)
cat(sprintf("Job status: %s\n", status))

# For meta-jobs
meta_status <- get_meta_job_status(config, meta_job_hash)
cat(sprintf("Meta-job status: %s\n", meta_status$status))
cat(sprintf("Current step: %d/%d\n", meta_status$current_step, length(meta_status$chain)))

# For pipelines
pipeline_status <- get_pipeline_status(config, pipeline_hash)
cat(sprintf("Pipeline status: %s\n", pipeline_status$status))
cat(sprintf("Progress: %.1f%% (%d/%d nodes)\n", 
            pipeline_status$progress * 100,
            pipeline_status$completed_nodes,
            pipeline_status$total_nodes))
```

---

## Troubleshooting

### Common Issues

1. **"File not found" error**
   - **Solution**: Verify files are uploaded and have status "completed"
   - Check file hashes are correct
   - Ensure files exist before submitting jobs

2. **"Validation error"**
   - **Solution**: Check that input names match method expectations
   - Verify parameters are correct type and format
   - Review method documentation for required inputs

3. **"Reference not found" ($ref errors)**
   - **Solution**: Ensure referenced step/node completed successfully
   - Verify path exists in output JSON structure
   - Check that dependencies are correctly specified

4. **"Pipeline previously failed"**
   - **Solution**: The system remembers failed pipelines
   - If you fixed the issue, the system will retry automatically
   - For validation errors, the system allows retry

5. **Unexpected output format**
   - **Solution**: Check method documentation for output structure
   - Use `str()` or `print()` to inspect output structure
   - Verify JSON parsing is enabled (`parse_json = TRUE`)

### Getting Help

1. Check API logs for detailed error messages
2. Verify all files are in "completed" status
3. Review method documentation for input/output formats
4. Test with simple examples first, then build complexity
5. Use debugging functions to inspect intermediate states

---

## Summary of Main Functions

### Jobs

- `query_job_by_hash(config, file_hash, method_name, parameters, file_inputs, validate_parameters)`
- `wait_for_job_results_by_hash(config, file_hash, method_name, parameters, timeout, interval, parse_json, validate_parameters, file_inputs)`
- `get_job_status_by_hash(config, file_hash, method_name, parameters, file_inputs)`

### Meta-Jobs

- `submit_meta_job_by_hash(config, file_hash, method_chain)`
- `submit_meta_job_multi(config, file_inputs, method_chain)`
- `wait_for_meta_job_results(config, meta_job_hash, timeout, interval, parse_json)`
- `get_meta_job_status(config, meta_job_hash)`

### Pipelines

- `submit_pipeline(config, pipeline_definition)`
- `wait_for_pipeline(config, pipeline_hash, timeout, parse_json)`
- `get_pipeline_status(config, pipeline_hash)`
- `create_pipeline_node(chain, dependencies, input_file_hash)`

### Utilities

- `upload_file(config, content, filename, chunk_size_mb, show_progress)`
- `hash_content(content)`
- `sort_file_inputs(file_inputs)`
- `sort_parameters(parameters)`

---

## Important Notes

1. **Exactly one of `file_hash` or `file_inputs`**: You cannot use both simultaneously. Use `NULL` for the one you're not using.

2. **Arrays preserve order**: The order of elements in an array is important and preserved exactly as you specify.

3. **Automatic deduplication**: The system automatically detects identical jobs/meta-jobs/pipelines and reuses existing results.

4. **References resolved automatically**: `$ref:` references are automatically resolved before executing each step.

5. **Pipelines require single terminal node**: All pipelines must converge to exactly one final node.

6. **File status matters**: Files must be in "completed" status before they can be used as inputs.
