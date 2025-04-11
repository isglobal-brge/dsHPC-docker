"""
Utility functions for text analysis.
"""

from collections import Counter

def count_lines(text):
    """Count the number of lines in the text."""
    return len(text.strip().split('\n'))

def count_words(text):
    """Count the number of words in the text."""
    return len(text.split())

def char_frequency(text):
    """Calculate the frequency of each character in the text."""
    # Ignore whitespace for frequency analysis
    text_no_whitespace = ''.join(text.split())
    return dict(Counter(text_no_whitespace)) 