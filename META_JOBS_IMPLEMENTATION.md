# Meta-Jobs System Implementation

## Overview
The meta-jobs system enables chaining multiple processing methods where each step's output becomes the input for the next step. The system automatically deduplicates work by reusing cached results from previous jobs.

## Key Features

### 1. **Automatic Result Caching**
- Every job output is automatically hashed and stored as a file
- Results are stored in the files database with GridFS support for large outputs
- Each job records its `output_file_hash` for future reference

### 2. **Intelligent Deduplication**
- Before running each step, the system checks if an identical job exists (same input + method + parameters)
- If a completed job exists, its output is reused without re-execution
- This enables efficient sharing of common preprocessing steps

### 3. **Chain Processing**
- Methods are executed sequentially
- Each step's output file hash becomes the next step's input
- The chain tracks which steps used cached results vs new execution

### 4. **API Endpoints**

#### Submit Meta-Job
```
POST /submit-meta-job
{
  "initial_file_hash": "abc123...",
  "method_chain": [
    {
      "method_name": "lung_mask",
      "parameters": {}
    },
    {
      "method_name": "extract_radiomics", 
      "parameters": {"feature_set": "all"}
    }
  ]
}
```

#### Get Meta-Job Status
```
GET /meta-job/{meta_job_id}
```

Returns detailed status including:
- Current processing step
- Which steps used cached results
- Final output when completed

## Architecture

### Backend Components

1. **Models** (`/src/dshpc_api/models/meta_job.py`)
   - `MetaJobRequest`: Input specification
   - `MetaJobResponse`: Submission response
   - `MetaJobInfo`: Full status information

2. **Service** (`/src/dshpc_api/services/meta_job_service.py`)
   - `submit_meta_job()`: Creates and starts processing
   - `process_meta_job_chain()`: Async chain processor
   - `get_meta_job_info()`: Status retrieval

3. **Job Output Upload** (`/src/slurm_api/services/job_service.py`)
   - `upload_job_output_as_file()`: Automatically stores job outputs as files
   - Integrated into `process_job_output()` for all completed jobs

4. **Database** 
   - Uses `meta_jobs` collection in same database as jobs
   - Leverages existing files database with GridFS

### R Package Support

New functions in `/Users/david/Documents/GitHub/dsHPC/R/07_meta.R`:

1. **`submit_meta_job()`**: Submit a processing chain
2. **`get_meta_job_status()`**: Check progress
3. **`wait_for_meta_job_results()`**: Wait for completion
4. **`execute_processing_chain()`**: All-in-one convenience function
5. **`get_meta_job_cache_info()`**: See which steps were cached

## Example Usage

### R Example
```r
config <- create_api_config("http://localhost", 9000, "api_key")
lung_image <- readRDS("lung_scan.rds")

# Define processing chain
chain <- list(
  list(method_name = "lung_mask", parameters = list()),
  list(method_name = "extract_radiomics", parameters = list(feature_set = "all"))
)

# Execute chain
results <- execute_processing_chain(config, lung_image, chain)
```

### Deduplication Example

1. User A runs: lung_image → lung_mask → radiomics_v1
2. User B runs: lung_image → lung_mask → radiomics_v2
   - Step 1 (lung_mask) is **reused** from User A's job
   - Step 2 (radiomics_v2) runs fresh

3. User C runs: lung_image → lung_mask → radiomics_v1
   - Both steps are **reused** - instant results!

## Benefits

1. **No Redundant Processing**: Common steps are computed once
2. **Automatic Optimization**: System handles caching transparently
3. **Unlimited Result Sizes**: GridFS handles outputs of any size
4. **Full Traceability**: Track which jobs produced which results
5. **Backward Compatible**: Existing single-job APIs unchanged

## Technical Details

### Deduplication Key
Jobs are uniquely identified by:
- Input file hash
- Method/function hash  
- Sorted parameters

### Storage Strategy
- Outputs < 15MB: Stored inline in MongoDB
- Outputs ≥ 15MB: Stored in GridFS
- All outputs get a SHA256 hash for identification

### Processing Flow
1. Validate all methods exist and are functional
2. Create meta-job record
3. For each step:
   - Check for existing completed job
   - If found: use cached output_file_hash
   - If not: submit new job and wait
   - Store output as file if not already stored
4. Return final output to user

## Error Handling

- Failed steps cause the entire chain to fail
- Stalled jobs are detected and marked as failed
- Each step has independent timeout handling
- Partial results are preserved for debugging
