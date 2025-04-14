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
