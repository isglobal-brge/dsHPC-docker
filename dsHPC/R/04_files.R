#' Upload a file only if it doesn't already exist
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file to upload
#' @param content_type MIME type of the file (e.g. "image/jpeg", "text/csv")
#' @param metadata Optional metadata for the file (named list)
#'
#' @return TRUE if the file was successfully uploaded or already exists
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' upload_file(config, "data.csv", content_type = "text/csv")
#' }
upload_file <- function(config, file_path, content_type = NULL, metadata = list()) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  if (!file.exists(file_path)) {
    stop(paste0("File not found: ", file_path))
  }
  
  # Compute file hash
  file_hash <- hash_file(file_path)
  
  # Check if file already exists
  if (hash_exists(config, file_hash)) {
    message("File already exists in the database.")
    return(TRUE)
  }
  
  # Determine filename from file_path
  filename <- basename(file_path)
  
  # Guess content type if not provided
  if (is.null(content_type)) {
    file_ext <- tolower(tools::file_ext(filename))
    content_type <- switch(file_ext,
      "jpg" = , "jpeg" = "image/jpeg",
      "png" = "image/png",
      "gif" = "image/gif",
      "pdf" = "application/pdf",
      "csv" = "text/csv",
      "json" = "application/json",
      "zip" = "application/zip",
      "txt" = "text/plain",
      "application/octet-stream"  # Default content type
    )
  }
  
  # Read file content as binary
  file_content <- readBin(file_path, "raw", file.info(file_path)$size)
  
  # Base64 encode the content
  content_base64 <- base64enc::base64encode(file_content)
  
  # Create request body
  body <- list(
    file_hash = file_hash,
    content = content_base64,
    filename = filename,
    content_type = content_type,
    metadata = metadata
  )
  
  # Upload the file
  tryCatch({
    response <- api_post(config, "/files/upload", body = body)
    
    # If we get here, upload was successful
    message("File uploaded successfully.")
    return(TRUE)
  }, error = function(e) {
    stop(paste0("Error uploading file: ", e$message))
  })
} 