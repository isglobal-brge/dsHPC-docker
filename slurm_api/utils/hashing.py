import hashlib

def mock_function_hash(script: str) -> str:
    """Mock function to generate a function hash."""
    return hashlib.md5(script.encode()).hexdigest()

def mock_file_hash() -> str | None:
    """Mock function to generate a file hash."""
    return None 