"""Small shared transform helpers used across the ingestion scripts."""


def round5(value):
    """Round a numeric value to 5 decimals; pass None through untouched."""
    return round(value, 5) if value is not None else None
