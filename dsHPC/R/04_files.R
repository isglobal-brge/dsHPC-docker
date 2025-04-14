#' Upload content to the HPC resource
#'
#' @param config API configuration created by create_api_config
#' @param content Content to upload (raw vector, character, or other object)
#' @param filename Name to use for the uploaded content
#'
#' @return TRUE if the content was successfully uploaded or already exists
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' upload_file(config, content, "hello.txt")
#' }
upload_file <- function(config, content, filename) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  # Convert content to raw bytes if it's not already
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  
  # Compute content hash using the dedicated function
  file_hash <- hash_content(content)
  
  # Check if content already exists
  if (hash_exists(config, file_hash)) {
    message("Content already exists in the database.")
    return(TRUE)
  }
  
  # Base64 encode the content
  content_base64 <- base64enc::base64encode(content)
  
  # Create request body
  body <- list(
    file_hash = file_hash,
    content = content_base64,
    filename = filename,
    content_type = "application/octet-stream"
  )
  
  # Upload the content
  tryCatch({
    response <- api_post(config, "/files/upload", body = body)
    
    # If we get here, upload was successful
    message("Content uploaded successfully.")
    return(TRUE)
  }, error = function(e) {
    stop(paste0("Error uploading content: ", e$message))
  })
} 