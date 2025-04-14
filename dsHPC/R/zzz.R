#' @importFrom R6 R6Class
#' @importFrom methods is
NULL

.onAttach <- function(lib, pkg) {
  # Helper function to register resource resolvers
  registerResolver <- function(res) {
    # Get class name of resolver for status message
    class <- class(res)[[1]]
    
    # Display registration status message
    packageStartupMessage(paste0("Registering ", class, "..."))
    
    # Register the resolver
    resourcer::registerResourceResolver(res)
  }
  
  # Create and register HPC resolver on package attach
  registerResolver(HPCResourceResolver$new())
}

.onDetach <- function(lib) {
  # Clean up by unregistering the HPC resolver
  resourcer::unregisterResourceResolver("HPCResourceResolver")
}
