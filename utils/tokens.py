import tiktoken
from core.config import Config

encoding = tiktoken.encoding_for_model(Config.OPENAI_MODEL)

def count_tokens(text: str) -> int:
    """Return number of tokens in a string."""
    return len(encoding.encode(text))