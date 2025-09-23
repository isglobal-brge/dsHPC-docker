import logging
import os
import sys

# ANSI color codes for colorful logs
COLORS = {
    'BLACK': '\033[0;30m',
    'RED': '\033[0;31m',
    'GREEN': '\033[0;32m',
    'YELLOW': '\033[0;33m',
    'BLUE': '\033[0;34m',
    'MAGENTA': '\033[0;35m',
    'CYAN': '\033[0;36m',
    'WHITE': '\033[0;37m',
    'BOLD_BLACK': '\033[1;30m',
    'BOLD_RED': '\033[1;31m',
    'BOLD_GREEN': '\033[1;32m',
    'BOLD_YELLOW': '\033[1;33m',
    'BOLD_BLUE': '\033[1;34m',
    'BOLD_MAGENTA': '\033[1;35m',
    'BOLD_CYAN': '\033[1;36m',
    'BOLD_WHITE': '\033[1;37m',
    'RESET': '\033[0m',
}

# Custom formatter to add colors based on log level
class ColoredFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG: COLORS['BLUE'] + '%(asctime)s ' + COLORS['BOLD_BLUE'] + '[%(levelname)s]' + COLORS['RESET'] + ' %(name)s: %(message)s',
        logging.INFO: COLORS['GREEN'] + '%(asctime)s ' + COLORS['BOLD_GREEN'] + '[%(levelname)s]' + COLORS['RESET'] + ' %(name)s: ' + COLORS['CYAN'] + '%(message)s' + COLORS['RESET'],
        logging.WARNING: COLORS['YELLOW'] + '%(asctime)s ' + COLORS['BOLD_YELLOW'] + '[%(levelname)s]' + COLORS['RESET'] + ' %(name)s: %(message)s',
        logging.ERROR: COLORS['RED'] + '%(asctime)s ' + COLORS['BOLD_RED'] + '[%(levelname)s]' + COLORS['RESET'] + ' %(name)s: %(message)s',
        logging.CRITICAL: COLORS['BOLD_RED'] + '%(asctime)s [%(levelname)s] %(name)s: %(message)s' + COLORS['RESET'],
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

# Configure logging
def setup_logging():
    # Set format for different log levels
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    
    # Configure the root logger
    logging.basicConfig(level=getattr(logging, log_level))
    
    # Get the root logger and set its formatter
    root_logger = logging.getLogger()
    
    # Set higher log level for some verbose modules
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('uvicorn').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    # Create our module logger with custom level
    logger = logging.getLogger(__name__)
    
    # Clear existing handlers and add our custom handler
    if root_logger.handlers:
        handler = root_logger.handlers[0]
        handler.setFormatter(ColoredFormatter())
    
    return logger

# Initialize logger
logger = setup_logging() 