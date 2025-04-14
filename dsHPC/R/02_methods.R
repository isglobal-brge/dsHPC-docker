#' Get available methods from the API
#'
#' @param config API configuration created by create_api_config
#'
#' @return A named list of methods and their parameters
#'
#' @examples
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' methods <- get_methods(config)
get_methods <- function(config) {
  # Make the API call to get methods
  response <- api_get(config, "/methods")
  
  # Check if methods exist in the response
  if (is.null(response$methods) || nrow(response$methods) == 0) {
    return(list())
  }
  
  # The methods are returned as a data frame
  methods_df <- response$methods
  
  # Convert each row to a list element
  methods <- list()
  for (i in 1:nrow(methods_df)) {
    method_name <- methods_df$name[i]
    methods[[method_name]] <- as.list(methods_df[i,])
  }
  
  return(methods)
}

#' Validate method parameters against method specification
#'
#' @param method_name The name of the method to validate against
#' @param params A named list of parameters to validate
#' @param method_spec The method specification as returned by get_methods()
#'
#' @return TRUE if validation passes, otherwise throws an error with details
#'
#' @examples
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' methods <- get_methods(config)
#' params <- list(threshold = 50)
#' validate_method_parameters("count_black_pixels", params, methods)
validate_method_parameters <- function(method_name, params, method_spec) {
  # Check if method exists in specification
  if (!method_name %in% names(method_spec)) {
    stop(paste0("Method '", method_name, "' not found in API methods."))
  }
  
  # Get method parameters specification
  method_info <- method_spec[[method_name]]
  
  # Check if method has parameters defined
  if (is.null(method_info$parameters) || length(method_info$parameters) == 0) {
    # If no parameters defined but params provided
    if (length(params) > 0) {
      stop(paste0("Method '", method_name, "' does not accept parameters, but parameters were provided."))
    }
    return(TRUE)
  }
  
  # Extract parameter info from the parameter data frame
  param_df <- method_info$parameters[[1]]
  
  if (is.null(param_df) || nrow(param_df) == 0) {
    # No parameters defined
    if (length(params) > 0) {
      stop(paste0("Method '", method_name, "' does not accept parameters, but parameters were provided."))
    }
    return(TRUE)
  }
  
  # Create a map of parameter names to their info
  method_params <- list()
  for (i in 1:nrow(param_df)) {
    param_name <- param_df$name[i]
    method_params[[param_name]] <- as.list(param_df[i,])
  }
  
  # Check for unexpected parameters
  unexpected_params <- setdiff(names(params), names(method_params))
  if (length(unexpected_params) > 0) {
    stop(paste0("Unexpected parameters provided: ", 
                paste(unexpected_params, collapse = ", "), "."))
  }
  
  # Check for required parameters
  missing_required <- character(0)
  
  for (param_name in names(method_params)) {
    param_info <- method_params[[param_name]]
    if (!is.null(param_info$required) && param_info$required && 
        (!param_name %in% names(params) || is.null(params[[param_name]]))) {
      missing_required <- c(missing_required, param_name)
    }
  }
  
  if (length(missing_required) > 0) {
    stop(paste0("Missing required parameters: ", 
                paste(missing_required, collapse = ", "), "."))
  }
  
  return(TRUE)
}
