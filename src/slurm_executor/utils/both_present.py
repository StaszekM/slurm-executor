import re


def both_present(a: re.Pattern[str], b: re.Pattern[str]) -> re.Pattern[str]:
    """Combine two regex patterns to form pattern that checks if both
    are present in a string, no matter order of occurrence."""
    return re.compile(rf"(?=.*{a.pattern})(?=.*{b.pattern})")
