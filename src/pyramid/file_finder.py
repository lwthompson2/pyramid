from pathlib import Path


class FileFinder():
    """Locate files with respect to a search path -- ie a list of path prefixes."""

    def __init__(
        self,
        search_path: list[str] = []
    ) -> None:
        self.search_path = search_path

    def __eq__(self, other: object) -> bool:
        """Compare FileFinders field-wise, to support tests."""
        if isinstance(other, self.__class__):
            return self.search_path == other.search_path
        else:  # pragma: no cover
            return False

    def find(self, original: str) -> str:
        """Locate the given original file or directory path with respect to this FileFinder's search_path.

        The search rules are:
         - If original is not a string, return it as-is.
         - If original is an absolute path, return it with any user folder (eg "~") expanded.
         - For each element of search_path p, in order, check the following:
           - Does original, prepended with p, with any user folder expanded, exist?
           - If so, return the path that exists.
         - If none of the search_path prefixes yields a match, return the original with any user folder expanded.
        """
        if not isinstance(original, str):
            return original

        original_path = Path(original).expanduser()
        if original_path.is_absolute():
            # Don't try to search for absolute paths, just use them.
            return original_path.as_posix()

        for prefix in self.search_path:
            prefixed_path = Path(prefix, original).expanduser()
            print(prefixed_path)
            if prefixed_path.exists():
                # Use the first matching prefix, if any.
                return prefixed_path.as_posix()

        # The original doesn't exist yet, but it's still useful to expand the user folder.
        return original_path.as_posix()
