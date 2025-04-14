#' Generate a hash based on content
#'
#' @param content The content to hash (raw vector, character, or other object)
#'
#' @return A character string with the content hash using SHA-256 algorithm
#'
#' @examples
#' \dontrun{
#' content <- "Hello World"
#' content_hash <- hash_content(content)
#' }
hash_content <- function(content) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("Package 'digest' is required. Please install it.")
  }
  
  # Convert content to raw bytes if it's not already
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  
  # Generate hash using SHA-256 algorithm
  hash <- digest::digest(content, algo = "sha256", serialize = FALSE)
  
  return(hash)
}

#' Check which hashes already exist in the database
#'
#' @param config API configuration created by create_api_config
#' @param hashes Character vector of content hashes to check
#'
#' @return A list with two components: existing_hashes and missing_hashes
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content1 <- "Hello World"
#' content2 <- "Goodbye World"
#' hash1 <- hash_content(content1)
#' hash2 <- hash_content(content2)
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
#' content <- "Hello World"
#' hash <- hash_content(content)
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
