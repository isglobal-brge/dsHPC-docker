#' Get available methods from the API
#'
#' @param config API configuration created by create_api_config
#'
#' @return A named list of methods and their parameters
#' @export
#'
#' @examples
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' methods <- get_methods(config)
get_methods <- function(config) {
  # Make the API call to get methods
  response <- api_get(config, "/methods")
  return(response)
}

#' Validate method parameters against method specification
#'
#' @param method_name The name of the method to validate against
#' @param params A named list of parameters to validate
#' @param method_spec The method specification as returned by get_methods()
#'
#' @return TRUE if validation passes, otherwise throws an error with details
#' @export
#'
#' @examples
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' methods <- get_methods(config)
#' params <- list(param1 = "value1", param2 = "value2")
#' validate_method_parameters("example_method", params, methods)
validate_method_parameters <- function(method_name, params, method_spec) {
  # Check if method exists in specification
  if (!method_name %in% names(method_spec)) {
    stop(paste0("Method '", method_name, "' not found in API methods."))
  }
  
  # Get method parameters specification
  method_info <- method_spec[[method_name]]
  
  # Check if method has parameters defined
  if (!"parameters" %in% names(method_info) || length(method_info$parameters) == 0) {
    # If no parameters defined but params provided
    if (length(params) > 0) {
      stop(paste0("Method '", method_name, "' does not accept parameters, but parameters were provided."))
    }
    return(TRUE)
  }
  
  method_params <- method_info$parameters
  
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
