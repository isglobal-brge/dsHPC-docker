#' Example Custom API Resource Client
#'
#' This is an example of how to extend the HPCResourceClient to support a different format.
#'
#' @description
#' CustomAPIClient extends HPCResourceClient but accepts resources in "custom.api" format.
#' This demonstrates how to subclass HPCResourceClient to create clients for specialized APIs.
#'
#' @examples
#' \dontrun{
#' # Create a resource object with custom format
#' resource <- list(
#'   url = "http://localhost:9000",
#'   format = "custom.api",
#'   secret = "custom_api_key"
#' )
#'
#' # Initialize client
#' client <- CustomAPIClient$new(resource)
#'
#' # Access API methods
#' methods <- client$getMethods()
#' }
#'
#' @export
CustomAPIClient <- R6::R6Class(
  "CustomAPIClient",
  inherit = HPCResourceClient,
  
  private = list(
    # Override the expected formats to accept "custom.api" format
    .expected_formats = c("custom.api")
    
    # You can also override other private methods like .getAPIConfig if needed
    # to customize how the configuration is created
  )
)

#' Create a custom resolver for Custom API resources
#'
#' @description
#' This class demonstrates how to create a custom resolver for a specialized API client.
#' It creates CustomAPIClient instances for resources with "custom.api" format.
#'
#' @examples
#' \dontrun{
#' # Register the resolver
#' resolver <- CustomAPIResolver$new()
#' resourcer::registerResourceResolver(resolver)
#'
#' # Create a resource
#' resource <- list(
#'   url = "http://localhost:9000",
#'   format = "custom.api",
#'   secret = "custom_api_key"
#' )
#'
#' # The resolver will automatically use the CustomAPIClient
#' is_suitable <- resolver$isFor(resource)  # TRUE
#' }
#'
#' @export
CustomAPIResolver <- R6::R6Class(
  "CustomAPIResolver",
  inherit = resourcer::ResourceResolver,
  public = list(
    #' @description
    #' Check if a resource is suitable for Custom API handling
    #'
    #' @param resource Resource configuration to validate
    #' @return Logical indicating if resource is suitable
    isFor = function(resource) {
      isSuitable <- super$isFor(resource) &&
        tolower(resource$format) %in% c("custom.api")
      return(isSuitable)
    },

    #' @description
    #' Create new client for Custom API resource
    #'
    #' @param resource Resource configuration
    #' @return New CustomAPIClient instance or NULL if resource unsuitable
    newClient = function(resource) {
      if (self$isFor(resource)) {
        client <- CustomAPIClient$new(resource)
        return(client)
      } else {
        return(NULL)
      }
    }
  )
)

#' Get a Custom API client from a resource
#'
#' @param resource A list containing resource configuration (url, format, secret)
#'
#' @return A CustomAPIClient instance
#' @export
#'
#' @examples
#' \dontrun{
#' resource <- list(
#'   url = "http://localhost:9000",
#'   format = "custom.api",
#'   secret = "custom_api_key"
#' )
#'
#' api <- get_custom_api(resource)
#' }
get_custom_api <- function(resource) {
  CustomAPIClient$new(resource)
} 