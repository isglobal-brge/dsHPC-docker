#' HPC API Resource Client
#'
#' This R6 class manages connections and interactions with dsHPC API endpoints.
#' It extends the generic ResourceClient class to provide dsHPC-specific functionality.
#'
#' @description
#' The HPCResourceClient class provides methods to:
#' * Initialize connections with dsHPC API endpoints
#' * Access API methods and execute jobs
#'
#' @details
#' The class uses a URL-based configuration system with the format:
#' `http://host:port`
#'
#' @examples
#' \dontrun{
#' # Create a resource object
#' resource <- list(
#'   url = "http://localhost:9000",
#'   format = "dshpc.api",
#'   secret = "please_change_me"
#' )
#'
#' # Initialize client
#' client <- HPCResourceClient$new(resource)
#'
#' # Access API methods
#' methods <- client$getMethods()
#'
#' # Upload a file
#' client$uploadFile("data.csv", content_type = "text/csv")
#' }
#'
#' @importFrom R6 R6Class
#' @export
HPCResourceClient <- R6::R6Class(
  "HPCResourceClient",
  inherit = resourcer::ResourceClient,
  public = list(
    #' @description
    #' Initialize a new HPCResourceClient instance
    #'
    #' @param resource A list containing resource configuration (url, format, secret)
    #'
    #' @return A new HPCResourceClient object
    initialize = function(resource) {
      # Initialize parent class
      super$initialize(resource)
    },
    
    #' @description
    #' Get available methods from the API
    #'
    #' @return A list of available methods
    getMethods = function() {
      config <- private$.getAPIConfig()
      get_methods(config)
    },
    
    #' @description
    #' Upload a file to the API
    #'
    #' @param file_path Path to the file to upload
    #' @param content_type Content type of the file (optional)
    #' @param metadata Additional metadata (optional)
    #'
    #' @return TRUE if upload was successful
    uploadFile = function(file_path, content_type = NULL, metadata = list()) {
      config <- private$.getAPIConfig()
      upload_file(config, file_path, content_type, metadata)
    },
    
    #' @description
    #' Execute a job on the API
    #'
    #' @param file_path Path to the file to process
    #' @param method_name Name of the method to execute
    #' @param parameters Parameters for the method
    #' @param wait Whether to wait for job completion
    #' @param timeout Maximum time to wait (in seconds)
    #'
    #' @return Job information or results
    executeJob = function(file_path, method_name, parameters = list(), 
                          wait = FALSE, timeout = 300) {
      config <- private$.getAPIConfig()
      
      if (wait) {
        wait_for_job_results(config, file_path, method_name, parameters, timeout = timeout)
      } else {
        query_job(config, file_path, method_name, parameters)
      }
    },
    
    #' @description
    #' Get the status of a job
    #'
    #' @param file_path Path to the file being processed
    #' @param method_name Name of the method executed
    #' @param parameters Parameters used for the method
    #'
    #' @return The status of the job as a string
    getJobStatus = function(file_path, method_name, parameters = list()) {
      config <- private$.getAPIConfig()
      get_job_status(config, file_path, method_name, parameters)
    },
    
    #' @description
    #' Check if a job succeeded
    #'
    #' @param file_path Path to the file being processed
    #' @param method_name Name of the method executed
    #' @param parameters Parameters used for the method
    #'
    #' @return TRUE if the job completed successfully, FALSE otherwise
    jobSucceeded = function(file_path, method_name, parameters = list()) {
      config <- private$.getAPIConfig()
      job_succeeded(config, file_path, method_name, parameters)
    },
    
    #' @description
    #' Get the output of a completed job
    #'
    #' @param file_path Path to the file processed
    #' @param method_name Name of the method executed
    #' @param parameters Parameters used for the method
    #' @param parse_json Whether to parse the output as JSON (default: TRUE)
    #'
    #' @return The job output, parsed as JSON if requested
    getJobOutput = function(file_path, method_name, parameters = list(), parse_json = TRUE) {
      config <- private$.getAPIConfig()
      get_job_output(config, file_path, method_name, parameters, parse_json)
    },
    
    #' @description
    #' Wait for a job to complete and return results
    #'
    #' @param file_path Path to the file processed
    #' @param method_name Name of the method executed
    #' @param parameters Parameters used for the method
    #' @param timeout Maximum time to wait in seconds (default: 300)
    #' @param interval Polling interval in seconds (default: 5)
    #' @param parse_json Whether to parse the output as JSON (default: TRUE)
    #'
    #' @return The job output if completed within timeout, otherwise throws an error
    waitForJob = function(file_path, method_name, parameters = list(), 
                          timeout = 300, interval = 5, parse_json = TRUE) {
      config <- private$.getAPIConfig()
      wait_for_job_results(config, file_path, method_name, parameters, 
                          timeout, interval, parse_json)
    }
  ),
  
  private = list(
    #' Get API configuration for use with dsHPC functions
    #'
    #' This method creates an API configuration object using the resource information
    .getAPIConfig = function() {
      resource <- super$getResource()
      
      # Extract base URL parts
      url_parts <- strsplit(resource$url, "://")[[1]]
      protocol <- url_parts[1]
      
      # Extract host and port
      conn_parts <- strsplit(url_parts[2], ":")[[1]]
      host <- conn_parts[1]
      port <- as.integer(conn_parts[2])
      
      # Get API key from secret
      api_key <- resource$secret
      
      # Create and return API configuration
      config <- create_api_config(
        base_url = paste0(protocol, "://", host),
        port = port,
        api_key = api_key,
        auth_header = "X-API-Key",
        auth_prefix = ""
      )
      
      return(config)
    }
  )
)

#' HPC API Resource Resolver
#'
#' This R6 class handles resolution of dsHPC API resources. It validates
#' resource configurations and creates appropriate client instances for interacting
#' with dsHPC API endpoints.
#'
#' @description
#' The resolver performs two main functions:
#' 1. Validates if a resource configuration is suitable for dsHPC API
#' 2. Creates new HPCResourceClient instances for valid resources
#'
#' @examples
#' \dontrun{
#' resolver <- HPCResourceResolver$new()
#'
#' # Check if resource is suitable
#' resource <- list(
#'   url = "http://localhost:9000",
#'   format = "dshpc.api"
#' )
#' is_suitable <- resolver$isFor(resource)
#'
#' # Create client if suitable
#' client <- resolver$newClient(resource)
#' }
#'
#' @export
HPCResourceResolver <- R6::R6Class(
  "HPCResourceResolver",
  inherit = resourcer::ResourceResolver,
  public = list(
    #' @description
    #' Check if a resource is suitable for dsHPC API handling
    #'
    #' @param resource Resource configuration to validate
    #' @return Logical indicating if resource is suitable
    isFor = function(resource) {
      isSuitable <- super$isFor(resource) &&
        tolower(resource$format) %in% c("dshpc.api")
      return(isSuitable)
    },

    #' @description
    #' Create new client for dsHPC API resource
    #'
    #' @param resource Resource configuration
    #' @return New HPCResourceClient instance or NULL if resource unsuitable
    newClient = function(resource) {
      if (self$isFor(resource)) {
        client <- HPCResourceClient$new(resource)
        return(client)
      } else {
        return(NULL)
      }
    }
  )
)
