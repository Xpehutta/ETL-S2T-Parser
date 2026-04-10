import os

def load_skills() -> str:
    """Load skills.md from the project root."""
    try:
        with open("skills.md", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""

def load_tools() -> str:
    """Load tools.md from the project root."""
    try:
        with open("tools.md", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""