import tiktoken
import os

encoding = tiktoken.encoding_for_model(os.getenv("OPENAI_MODEL", ""))

def count_tokens(text: str) -> int:
    """Return number of tokens in a string."""
    return len(encoding.encode(text))