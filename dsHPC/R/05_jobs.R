#' Query a job execution
#'
#' @param config API configuration created by create_api_config
#' @param content Content to process (raw vector, character, or other object)
#' @param method_name Name of the method to execute
#' @param parameters Named list of parameters for the method
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return A list with job information including status, output, and error details
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' job <- query_job(config, content, "analyze_data", list(parameter1 = "value1"))
#' }
query_job <- function(config, content, method_name, parameters = list(), validate_parameters = TRUE) {
  # Calculate content hash
  file_hash <- hash_content(content)
  
  # Validate the inputs
  if (!is.character(method_name) || length(method_name) != 1) {
    stop("method_name must be a single character string")
  }
  
  if (!is.list(parameters)) {
    stop("parameters must be a named list")
  }
  
  # Sort parameters alphabetically to ensure consistent ordering
  sorted_params <- sort_parameters(parameters)
  
  # Validate parameters against method specification if requested
  if (validate_parameters) {
    # Get all available methods
    methods <- get_methods(config)
    
    # Validate parameters against method specification (use original parameters for validation)
    validate_method_parameters(method_name, parameters, methods)
  }
  
  # Create request body with sorted parameters
  body <- list(
    file_hash = file_hash,
    method_name = method_name,
    parameters = sorted_params
  )
  
  # Make API call
  response <- api_post(config, "/query-job", body = body)
  
  return(response)
}

#' Get the status of a job
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file processed
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#'
#' @return The status of the job as a string
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' status <- get_job_status(config, "image.jpg", "count_black_pixels", list(threshold = 30))
#' }
get_job_status <- function(config, file_path, method_name, parameters = list()) {
  # Query the job
  job_info <- query_job(config, file_path, method_name, parameters, validate_parameters = FALSE)
  
  # Return just the status
  return(job_info$status)
}

#' Check if a job succeeded
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file processed
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#'
#' @return TRUE if the job completed successfully, FALSE otherwise
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' if (job_succeeded(config, "image.jpg", "count_black_pixels", list(threshold = 30))) {
#'   print("Job completed successfully")
#' }
#' }
job_succeeded <- function(config, file_path, method_name, parameters = list()) {
  # Query the job
  job_info <- query_job(config, file_path, method_name, parameters, validate_parameters = FALSE)
  
  # Check if status is "CD" (completed)
  return(!is.null(job_info$status) && job_info$status == "CD")
}

#' Get the output of a completed job
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file processed
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#' @param parse_json Whether to parse the output as JSON (default: TRUE)
#'
#' @return The job output, parsed as JSON if requested
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' output <- get_job_output(config, "image.jpg", "count_black_pixels", list(threshold = 30))
#' }
get_job_output <- function(config, file_path, method_name, parameters = list(), parse_json = TRUE) {
  # Query the job
  job_info <- query_job(config, file_path, method_name, parameters, validate_parameters = FALSE)
  
  # Check if job is completed
  if (is.null(job_info$status) || job_info$status != "CD") {
    stop(paste0("Cannot get output: job is not completed. Current status: ", 
                ifelse(is.null(job_info$status), "unknown", job_info$status)))
  }
  
  # Get the output
  output <- job_info$output
  
  # Parse JSON if requested
  if (parse_json && !is.null(output) && output != "") {
    tryCatch({
      output <- jsonlite::fromJSON(output)
    }, error = function(e) {
      warning("Failed to parse output as JSON: ", e$message)
      # Return the raw output instead
    })
  }
  
  return(output)
}

#' Wait for a job to complete and return results
#'
#' @param config API configuration created by create_api_config
#' @param content Content to process (raw vector, character, or other object)
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#' @param timeout Maximum time to wait in seconds (default: 300)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the output as JSON (default: TRUE)
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return The job output if completed within timeout, otherwise throws an error
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' results <- wait_for_job_results(config, content, "analyze_text", list(parameter1 = "value1"))
#' }
wait_for_job_results <- function(config, content, method_name, parameters = list(), 
                                 timeout = 300, interval = 5, parse_json = TRUE,
                                 validate_parameters = TRUE) {
  # Calculate content hash once for efficiency
  file_hash <- hash_content(content)
  
  # Ensure interval is not too small
  interval <- max(interval, 1)
  
  # Start timer
  start_time <- Sys.time()
  
  # Sort parameters alphabetically to ensure consistent ordering
  sorted_params <- sort_parameters(parameters)
  
  # Validate parameters on first call
  if (validate_parameters) {
    # Get all available methods
    methods <- get_methods(config)
    
    # Validate parameters against method specification (use original parameters for validation)
    validate_method_parameters(method_name, parameters, methods)
  }
  
  # Create request body with sorted parameters
  body <- list(
    file_hash = file_hash,
    method_name = method_name,
    parameters = sorted_params
  )
  
  # Query the job initially
  job_info <- api_post(config, "/query-job", body = body)
  
  # Loop until timeout or job completion
  while(is.null(job_info$status) || job_info$status != "CD") {
    # Check for timeout
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    if (elapsed > timeout) {
      stop(paste0("Timeout waiting for job to complete. Current status: ", 
                  ifelse(is.null(job_info$status), "unknown", job_info$status)))
    }
    
    # Check for error state
    if (!is.null(job_info$status) && job_info$status == "ER") {
      stop(paste0("Job failed with error: ", 
                  ifelse(is.null(job_info$error_details), "unknown error", job_info$error_details)))
    }
    
    # Wait before polling again
    Sys.sleep(interval)
    
    # Poll job status without validation
    job_info <- api_post(config, "/query-job", body = body)
  }
  
  # Get the output
  output <- job_info$output
  
  # Parse JSON if requested
  if (parse_json && !is.null(output) && output != "") {
    tryCatch({
      output <- jsonlite::fromJSON(output)
    }, error = function(e) {
      warning("Failed to parse output as JSON: ", e$message)
      # Return the raw output instead
    })
  }
  
  return(output)
}
