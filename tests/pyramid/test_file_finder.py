from pathlib import Path

from pyramid.file_finder import FileFinder


def test_file_finder(tmp_path):
    # Set up some files to locate.
    search_a = Path(tmp_path, 'a')
    search_a.mkdir()
    foo_a = Path(search_a, "foo.txt")
    foo_a.touch()
    bar_a = Path(search_a, "bar.txt")
    bar_a.touch()

    search_b = Path(tmp_path, 'b')
    search_b.mkdir()
    bar_b = Path(search_b, "bar.txt")
    bar_b.touch()
    baz_b = Path(search_b, "baz.txt")
    baz_b.touch()

    file_finder = FileFinder([search_a.as_posix(), search_b.as_posix()])

    # Finding non-strings is nonsense to return as-is.
    assert file_finder.find(None) is None
    assert file_finder.find(0) == 0
    assert file_finder.find(123.456) == 123.456

    # Finding an absolute dir or file returns as-is, whether it exists or not.
    absolute_dir = Path(tmp_path, "extra").as_posix()
    assert file_finder.find(absolute_dir) == absolute_dir
    absolute_file = Path(tmp_path, "extra.txt").as_posix()
    assert file_finder.find(absolute_file) == absolute_file

    # User folder should get expanded and act like absolute.
    user_dir = Path("~", "extra")
    assert file_finder.find(user_dir.as_posix()) == user_dir.expanduser().as_posix()
    user_file = Path("~", "extra.txt")
    assert file_finder.find(user_file.as_posix()) == user_file.expanduser().as_posix()

    # Files should be located on the first matching path prefix.
    assert file_finder.find("foo.txt") == foo_a.as_posix()
    assert file_finder.find("bar.txt") == bar_a.as_posix()
    assert file_finder.find("baz.txt") == baz_b.as_posix()

    # Non-matching files should be returned as-is, or with user folder expanded.
    assert file_finder.find("nope.txt") == "nope.txt"
    assert file_finder.find("~/no_way.txt") == Path("~/no_way.txt").expanduser().as_posix()
