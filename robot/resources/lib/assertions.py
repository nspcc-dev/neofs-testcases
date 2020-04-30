#!/usr/bin/python3

"""
A file with specific assertions that Robot Framework
doesn't have in its builtins.
"""

from robot.api.deco import keyword
from robot.utils.asserts import assert_equal

@keyword('Should Be Equal as Binaries')
def sbe_as_binaries(fst: str, snd: str):
    """
    Assertion to compare binary contents of
    two files. Parameters:
    - `fst`: path to first file
    - `snd`: path to second file
    """
    fst_fd, snd_fd = open(fst, 'rb'), open(snd, 'rb')
    fst_bytes, snd_bytes = fst_fd.read(), snd_fd.read()
    assert_equal(fst_bytes, snd_bytes, msg='Given files are not equal as binaries')
