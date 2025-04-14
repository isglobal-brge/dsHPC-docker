#' Query a job execution
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the file to process
#' @param method_name Name of the method to execute
#' @param parameters Named list of parameters for the method
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return A list with job information including status, output, and error details
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- hash_file("data.csv")
#' job <- query_job(config, file_hash, "analyze_data", list(parameter1 = "value1"))
#' }
query_job <- function(config, file_hash, method_name, parameters = list(), validate_parameters = TRUE) {
  # Validate the inputs
  if (!is.character(file_hash) || length(file_hash) != 1) {
    stop("file_hash must be a single character string")
  }
  
  if (!is.character(method_name) || length(method_name) != 1) {
    stop("method_name must be a single character string")
  }
  
  if (!is.list(parameters)) {
    stop("parameters must be a named list")
  }
  
  # Validate parameters against method specification if requested
  if (validate_parameters) {
    # Get all available methods
    methods <- get_methods(config)
    
    # Validate parameters against method specification
    validate_method_parameters(method_name, parameters, methods)
  }
  
  # Create request body
  body <- list(
    file_hash = file_hash,
    method_name = method_name,
    parameters = parameters
  )
  
  # Make API call
  response <- api_post(config, "/query-job", body = body)
  
  return(response)
}
