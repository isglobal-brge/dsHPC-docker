#' Create an API client configuration
#'
#' @param base_url Base URL of the API (without port)
#' @param port Port number of the API
#' @param api_key API key for authentication
#' @param auth_header Name of the authentication header (default "Authorization")
#' @param auth_prefix Prefix for the API key in the authentication header (default "Bearer")
#' @param timeout Timeout in seconds for API requests (default 30)
#'
#' @return A list with API configuration
#'
#' @examples
#' api_config <- create_api_config("https://api.example.com", 8080, "your-api-key")
create_api_config <- function(base_url, port, api_key, auth_header = "Authorization", 
                              auth_prefix = "Bearer", timeout = 30) {
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Package 'httr' is required. Please install it.")
  }
  
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required. Please install it.")
  }
  
  # Remove trailing slash from base_url if present
  base_url <- sub("/$", "", base_url)
  
  # Build full URL with port
  full_url <- paste0(base_url, ":", port)
  
  # Return configuration object
  list(
    base_url = full_url,
    api_key = api_key,
    auth_header = auth_header,
    auth_prefix = auth_prefix,
    timeout = timeout
  )
}

#' Make an API request
#'
#' @param config API configuration created by create_api_config
#' @param endpoint API endpoint (starting with /)
#' @param method HTTP method ("GET" or "POST")
#' @param params List of query parameters
#' @param body Request body (for POST)
#'
#' @return API response parsed from JSON
#'
#' @examples
#' config <- create_api_config("https://api.example.com", 8080, "your-api-key")
#' response <- api_request(config, "/users", "GET")
api_request <- function(config, endpoint, method = "GET", params = list(), body = NULL) {
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Package 'httr' is required. Please install it.")
  }
  
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required. Please install it.")
  }
  
  # Ensure endpoint starts with /
  if (!startsWith(endpoint, "/")) {
    endpoint <- paste0("/", endpoint)
  }
  
  # Build full URL
  url <- paste0(config$base_url, endpoint)
  
  # Prepare headers
  headers <- c(
    "Content-Type" = "application/json",
    "Accept" = "application/json"
  )
  
  # Add authentication header if API key is provided
  if (!is.null(config$api_key) && config$api_key != "") {
    if (is.null(config$auth_prefix) || config$auth_prefix == "") {
      auth_value <- config$api_key
    } else {
      auth_value <- paste(config$auth_prefix, config$api_key)
    }
    
    headers[config$auth_header] <- auth_value
  }
  
  # Prepare body if provided
  if (!is.null(body)) {
    # Use jsonlite::toJSON with sorted_keys equivalent behavior
    # The auto_unbox ensures scalars are not wrapped in arrays
    body <- jsonlite::toJSON(body, auto_unbox = TRUE)
  }
  
  # Make request based on method
  response <- switch(
    toupper(method),
    "GET" = httr::GET(
      url = url,
      query = params,
      httr::add_headers(.headers = headers),
      httr::timeout(config$timeout)
    ),
    "POST" = httr::POST(
      url = url,
      query = params,
      httr::add_headers(.headers = headers),
      body = body,
      httr::timeout(config$timeout),
      encode = "json"
    ),
    stop("Unsupported HTTP method: ", method)
  )
  
  # Check for HTTP errors
  if (httr::http_error(response)) {
    status <- httr::status_code(response)
    content <- tryCatch({
      httr::content(response, "text", encoding = "UTF-8")
    }, error = function(e) {
      "Could not parse error response"
    })
    stop(paste0(httr::http_status(response)$message, " (HTTP ", status, ").\n", 
                "URL: ", url, "\n",
                "Response: ", content))
  }
  
  # Parse and return JSON response
  content <- httr::content(response, "text", encoding = "UTF-8")
  
  # Return empty list for empty responses
  if (content == "") {
    return(list())
  }
  
  # Parse JSON
  tryCatch({
    jsonlite::fromJSON(content)
  }, error = function(e) {
    warning("Could not parse JSON response: ", e$message)
    content
  })
}

#' GET request helper
#'
#' @param config API configuration
#' @param endpoint API endpoint
#' @param params Query parameters
#'
#' @return API response
api_get <- function(config, endpoint, params = list()) {
  api_request(config, endpoint, "GET", params)
}

#' POST request helper
#'
#' @param config API configuration
#' @param endpoint API endpoint
#' @param body Request body
#' @param params Query parameters
#'
#' @return API response
api_post <- function(config, endpoint, body, params = list()) {
  api_request(config, endpoint, "POST", params, body)
} 
