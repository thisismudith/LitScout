# server/logger.py

from colorama import Fore, Style, init as colorama_init
from datetime import datetime

# Initialize once for whole project
colorama_init(autoreset=True)


class ColorLogger:
    """
    Generic colorized logger with optional timestamps.

    Format:
        [NAME - TAG - TIMESTAMP] MESSAGE
        or
        [NAME - TAG] MESSAGE
    """


    # TAG colors
    COLOR_INFO = Fore.CYAN
    COLOR_SUCCESS = Fore.GREEN
    COLOR_ERROR = Fore.RED
    COLOR_WARN = Fore.YELLOW
    COLOR_CMD = Fore.MAGENTA


    def __init__(self, name: str = "", tag_color: str = Fore.CYAN, include_timestamps: bool = False):
        self.name = name.upper()
        self.include_name = bool(name)
        self.include_timestamps = include_timestamps
        self.tag_color = tag_color

    def _tag(self, label: str, color: str) -> str:
        """
        Build the tag portion: [NAME - LABEL - TIMESTAMP]
        """

        if self.include_name:
            name_part = f"{self.name} - "
        else:
            name_part = ""

        if self.include_timestamps:
            ts = datetime.now().strftime("%H:%M:%S")
            return f"{self.tag_color}[{name_part}{color}{label}{self.tag_color} - {ts}]{Style.RESET_ALL}"
        else:
            return f"{self.tag_color}[{name_part}{color}{label}{self.tag_color}]{Style.RESET_ALL}"

    def _print(self, tag: str, message: str, color: str = Fore.RESET):
        print(f"{tag} {color}{message}{Fore.RESET}{Style.RESET_ALL}")


    # Public Logging Method
    def info(self, message: str, use_color: bool = True):
        tag = self._tag("INFO", self.COLOR_INFO)
        self._print(tag, message, self.COLOR_INFO if use_color else Fore.RESET)

    def success(self, message: str, use_color: bool = True):
        tag = self._tag("SUCCESS", self.COLOR_SUCCESS)
        self._print(tag, message, self.COLOR_SUCCESS if use_color else Fore.RESET)

    def error(self, message: str, use_color: bool = True):
        tag = self._tag("ERROR", self.COLOR_ERROR)
        self._print(tag, message, self.COLOR_ERROR if use_color else Fore.RESET)

    def warn(self, message: str, use_color: bool = True):
        tag = self._tag("WARN", self.COLOR_WARN)
        self._print(tag, message, self.COLOR_WARN if use_color else Fore.RESET)

    def cmd(self, message: str, use_color: bool = True):
        tag = self._tag("CMD", self.COLOR_CMD)
        self._print(tag, message, self.COLOR_CMD if use_color else Fore.RESET)

    def banner(self, title: str, subtitle: str | None = None, color: str = None):
        """Prints a dynamic banner

        Args:
            title (str): The main title
            subtitle (str | None, optional): The subtitle. Defaults to None.
            color (str, optional): The color for the banner. Defaults to Fore.CYAN.
        """
        if color is None:
            color = self.tag_color

        lines = [title]
        if subtitle:
            lines.append(subtitle)

        max_len = max(len(line) for line in lines)
        padding = 2
        width = max_len + padding * 2

        top = "╔" + "═" * width + "╗"
        bottom = "╚" + "═" * width + "╝"

        print(color + Style.BRIGHT + top)
        for line in lines:
            left = (width - len(line)) // 2
            right = width - len(line) - left
            print(color + Style.BRIGHT + "║" + " " * left + line + " " * right + "║")
        print(color + Style.BRIGHT + bottom + Style.RESET_ALL)