"""General utils"""
from datetime import datetime

def date_today() -> str:
    """Today's date as yyyy-mm-dd"""
    return datetime.now().strftime("%Y-%m-%d")

def date_now() -> str:
    """Current time as Hour:Minutes"""
    return datetime.now().strftime("%H%M")
