# -*- coding: utf-8 -*-
"""
IO utils, such as Opener, that abstracts file handling
functionalities to work seamlessly with local files and files
in the cloud .
"""
from typing import Callable

from s3fs import S3FileSystem


class Opener:
    """
    Opener abstracts open function for local files and cloud located files .

    Methods
    -------
    open(path: str, mode: str) -> Callable
        Reads dataset from path .
    get_filesystem(destination: str) -> str
        Get filesystem from path .
    get_filetype(path: str) -> str
        Get file type from path .

    """

    @staticmethod
    def open(path: str, mode: str) -> Callable:
        """
        Reads data from path .

        Parameters
        ----------
        path : str
            Path to data .
        mode : str
            Variable mode for the opener
            (for example, r for read, w for write ...) .

        Returns
        -------
        Callable
            Correct "open function" for the path .

        """
        filesystem, stripped_path = Opener.get_filesystem(path)
        if filesystem == "aws":
            return S3FileSystem().open(stripped_path, mode)
        else:
            return open(stripped_path, mode)

    @staticmethod
    def get_filesystem(path: str) -> str:
        """
        Get filesystem from path .

        Parameters
        ----------
        path : str
            Path to the file .

        Returns
        -------
        str
            Filesystem from the path .
        str
            Path without prefix .

        """
        if path.startswith("s3://"):
            filesystem = "aws"
            path = path[5:]
        else:
            filesystem = "local"
        return filesystem, path

    @staticmethod
    def get_filetype(path: str) -> str:
        """
        Get file type from path .

        Parameters
        ----------
        path : str
            Path to the file .

        Returns
        -------
        str
            File type from the suffix of the path .

        """
        if path.endswith("csv"):
            return "csv"
        elif path.endswith("parquet"):
            return "parquet"
