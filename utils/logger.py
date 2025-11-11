import sys
from datetime import datetime

class Logger:
    COLORS = {
        "INFO": "\033[94m",    # Blue
        "SUCCESS": "\033[92m", # Green
        "WARNING": "\033[93m", # Yellow
        "ERROR": "\033[91m",   # Red
        "RESET": "\033[0m"
    }

    @staticmethod
    def _log(level, message):
        color = Logger.COLORS.get(level, "")
        reset = Logger.COLORS["RESET"]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{color}[{timestamp}] [{level}] {message}{reset}", file=sys.stdout)

    @classmethod
    def info(cls, message): cls._log("INFO", message)
    @classmethod
    def success(cls, message): cls._log("SUCCESS", message)
    @classmethod
    def warning(cls, message): cls._log("WARNING", message)
    @classmethod
    def error(cls, message): cls._log("ERROR", message)