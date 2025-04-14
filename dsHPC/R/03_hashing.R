#' Generate a hash based on file content
#'
#' @param file_path Path to the file to hash
#'
#' @return A character string with the file hash using SHA-256 algorithm
#'
#' @examples
#' \dontrun{
#' file_hash <- hash_file("path/to/file.txt")
#' }
hash_file <- function(file_path) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("Package 'digest' is required. Please install it.")
  }
  
  if (!file.exists(file_path)) {
    stop(paste0("File not found: ", file_path))
  }
  
  # Read the file as a raw binary vector
  file_content <- readBin(file_path, "raw", file.info(file_path)$size)
  
  # Generate hash using SHA-256 algorithm
  hash <- digest::digest(file_content, algo = "sha256", serialize = FALSE)
  
  return(hash)
}

#' Check which hashes already exist in the database
#'
#' @param config API configuration created by create_api_config
#' @param hashes Character vector of file hashes to check
#'
#' @return A list with two components: existing_hashes and missing_hashes
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' hash1 <- hash_file("file1.txt")
#' hash2 <- hash_file("file2.txt")
#' result <- check_existing_hashes(config, c(hash1, hash2))
#' }
check_existing_hashes <- function(config, hashes) {
  if (!is.character(hashes)) {
    stop("Hashes must be a character vector")
  }
  
  # Ensure hashes is a list for JSON serialization
  hashes_list <- as.list(hashes)
  
  # Create request body
  body <- list(
    hashes = hashes_list
  )
  
  # Make API call
  response <- api_post(config, "/files/check-hashes", body = body)
  
  return(response)
}

#' Check if a specific hash exists in the database
#'
#' @param config API configuration created by create_api_config
#' @param hash A single hash string to check
#'
#' @return Boolean indicating if the hash exists
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' hash <- hash_file("data.csv")
#' if (hash_exists(config, hash)) {
#'   print("Hash exists in database")
#' }
#' }
hash_exists <- function(config, hash) {
  if (!is.character(hash) || length(hash) != 1) {
    stop("Hash must be a single character string")
  }
  
  # Call check_existing_hashes with a list containing one hash
  result <- check_existing_hashes(config, c(hash))
  
  # Return TRUE if hash is in existing_hashes
  return(hash %in% result$existing_hashes)
}
