# -*- coding: utf-8 -*-
"""
Tests utils.py .
"""

from file_handler.opener import Opener


def test_get_filetype():
    """
    Tests get_filetype .

    Parameters
    ----------
    None

    Returns
    -------
    None

    """
    # Test
    assert Opener.get_filetype("test.parquet") == "parquet"
    assert Opener.get_filetype("test.csv") == "csv"
