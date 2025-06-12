#' Sort parameters alphabetically by name
#'
#' This function ensures that parameters are always in the same order
#' regardless of how they were specified by the user. This is important
#' for job deduplication as MongoDB's document comparison is order-sensitive.
#'
#' @param parameters A named list of parameters
#'
#' @return A named list with parameters sorted alphabetically by name
#' @export
#'
#' @examples
#' params <- list(c = 3, a = 1, b = 2)
#' sorted_params <- sort_parameters(params)
#' # Returns: list(a = 1, b = 2, c = 3)
sort_parameters <- function(parameters) {
  if (!is.list(parameters)) {
    stop("parameters must be a list")
  }
  
  # Return empty list if no parameters
  if (length(parameters) == 0) {
    return(list())
  }
  
  # Get parameter names
  param_names <- names(parameters)
  
  # Check if all parameters are named
  if (is.null(param_names) || any(param_names == "")) {
    stop("All parameters must be named")
  }
  
  # Sort parameter names alphabetically
  sorted_names <- sort(param_names)
  
  # Create new list with sorted parameters
  sorted_params <- list()
  for (name in sorted_names) {
    sorted_params[[name]] <- parameters[[name]]
  }
  
  return(sorted_params)
} 