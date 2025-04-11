"""
Utility functions for text transformation.
"""

def apply_case(text, case_type):
    """Apply case transformation to the text."""
    case_type = case_type.lower()
    if case_type == 'upper':
        return text.upper()
    elif case_type == 'lower':
        return text.lower()
    elif case_type == 'title':
        return text.title()
    elif case_type == 'none':
        return text
    else:
        # Return original text if case type is unknown
        print(f"Warning: Unknown case type '{case_type}', returning original text.")
        return text 