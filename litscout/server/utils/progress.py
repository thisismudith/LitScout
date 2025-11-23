# server/utils/progress.py

from typing import Optional
from tqdm import tqdm


class ProgressBar:
    """
    Thin wrapper around tqdm so we can standardize
    how progress bars look across the project.
    """

    def __init__(
        self,
        total: Optional[int] = None,
        desc: str = "",
        unit: str = "item",
        dynamic_ncols: bool = True,
    ):
        """
        Args:
            total: Estimated total count (can be None for indeterminate).
            desc: Description prefix (e.g. 'OpenAlex C41008148').
            unit: Unit label (e.g. 'paper', 'row').
            dynamic_ncols: Let tqdm auto-size to terminal width.
        """
        self._bar = tqdm(
            total=total,
            desc=desc,
            unit=unit,
            dynamic_ncols=dynamic_ncols,
        )

    def update(self, n: int = 1) -> None:
        self._bar.update(n)

    def close(self) -> None:
        self._bar.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def create_progress_bar(
    total: Optional[int],
    desc: str,
    unit: str = "item",
) -> ProgressBar:
    """
    Factory helper to create a ProgressBar with consistent styling.
    Usage:
        bar = create_progress_bar(total_estimated, "OpenAlex C41008148", unit="paper")
        bar.update()
        bar.close()
    """
    return ProgressBar(total=total, desc=desc, unit=unit)
