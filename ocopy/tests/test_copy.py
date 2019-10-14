from ocopy.copy import *
from ocopy.hash import get_hash
from ocopy.utils import folder_size


def test_get_hash(tmpdir):
    assert get_hash(Path('/dev/null')) == 'ef46db3751d8e999'

    p = tmpdir / 'test-äöüàéè.txt'
    p.write('éèà' * 1024 * 1024 * 16)
    assert get_hash(p) == '41568b54725a72dd'


def test_folder_size(tmpdir):
    p = tmpdir.mkdir('bla') / 'test-äöüàéè.txt'
    p.write('asdf' * 8)

    p = tmpdir / 'test2-äöüàéè.txt'
    p.write('xxxx' * 4)

    assert folder_size(tmpdir) == 48


def test_copy(tmpdir):
    src_file = tmpdir / 'test-äöüàéè.txt'
    file_size = 1024 * 1024 * 16
    src_file.write('x' * file_size)

    destinations = ['dst_1', 'dst_2', 'dst_3']
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / 'test' for d in destinations]

    assert copy(src_file, destinations) == '6878668a929c42c1'
    assert folder_size(tmpdir) == file_size * 4

    for d in destinations:
        d.remove()
    assert folder_size(tmpdir) == file_size

    assert copy_threaded(src_file, destinations) == '6878668a929c42c1'
    assert folder_size(tmpdir) == file_size * 4
