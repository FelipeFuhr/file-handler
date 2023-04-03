# -*- coding: utf-8 -*-
"""
Implementation of FileHandler class .
"""

from typing import Any, Dict

from pandas import DataFrame, read_csv, read_parquet
from yaml import safe_dump, safe_load

from file_handler.opener import Opener


class FileHandler:
    """
    Handler for IO related functionalities. It should allow the
    user to save files locally, or in the cloud .

    Attributes
    ----------
    supported_file_formats : List[str]
        Supported filetypes to save/read to/from .

    Methods
    -------
    read_dataset(path: str) -> DataFrame
        Reads dataset from path .
    save_dataset(
        dataset: DataFrame
        destination: str,
        format: str = "csv"
    ) -> None
        Saves dataset .
    read_yaml(path: str) -> Dict[str, Any]
        Reads yaml from path .
    save_yaml(data: Dict[str, Any], path: str) -> None
        Saves yaml to path .

    """

    # Supported filetypes to save/read to/from .
    supported_file_formats = ["csv", "parquet"]

    @staticmethod
    def read_dataset(path: str, data_type: str = "pandas") -> DataFrame:
        """
        Reads dataset from path .

        Parameters
        ----------
        path : str
            Path to dataset .
        data_type : str
            Type of the data to be read. Currently supports pandas or numpy .

        Returns
        -------
        DataFrame
            Dataset read from path .

        Raises
        ------
        ValueError
            If format passed is invalid .
        NotImplementedError
            If format passed was not implemented. This should not
            occur, because it would imply a format not implemented
            is in the supported_file_formats list .

        """
        filetype = Opener.get_filetype(path)
        supported_file_formats = FileHandler.supported_file_formats
        if filetype not in supported_file_formats:
            raise ValueError(
                f"File format not supported. Given: {filetype}. ",
                f"Expected one of: {supported_file_formats}",
            )
        elif filetype == "csv":
            with Opener.open(path=path, mode="rb") as fh:
                data = read_csv(fh)
        elif filetype == "parquet":
            with Opener.open(path=path, mode="rb") as fh:
                data = read_parquet(fh)
        else:
            raise NotImplementedError(
                f"Type {filetype} is not currently supported, ",
                "but is on the way!",
            )
        return data

    @staticmethod
    def save_dataset(dataset: DataFrame, path: str, file_format: str = "csv") -> None:
        """
        Saves dataset .

        Parameters
        ----------
        dataset : DataFrame
            Dataset to be saved .
        path: str
            Path to save the dataset .
        file_format: str
            Format of the file to be saved. Currently supports pandas or numpy .

        Raises
        ------
        ValueError
            If format passed is invalid .
        NotImplementedError
            If format passed was not implemented. This should not occur,
            because it would imply a format not implemented is in the
            supported_file_formats list .

        Returns
        -------
        None

        """
        supported_file_formats = FileHandler.supported_file_formats
        if file_format not in supported_file_formats:
            raise ValueError(
                "File format not supported. ",
                f"Given: {format}. Expected one of: {supported_file_formats}",
            )
        elif file_format == "csv":
            with Opener.open(path=path, mode="wb") as fh:
                dataset.to_csv(fh)
        elif file_format == "parquet":
            with Opener.open(path=path, mode="wb") as fh:
                dataset.to_parquet(fh)
        else:
            raise NotImplementedError(
                f"Type {file_format} is not currently supported, but is on the way!"
            )
        return

    @staticmethod
    def read_yaml(path: str) -> Dict[str, Any]:
        """
        Reads yaml from path .

        Parameters
        ----------
        path : str
            Path to yaml .

        Returns
        -------
        Dict[str, Any]
            Definition read from path .

        """
        with Opener.open(path=path, mode="r") as fh:
            data = safe_load(fh)
        return data

    @staticmethod
    def save_yaml(data: Dict[str, Any], path: str) -> None:
        """
        Saves yaml to path .

        Parameters
        ----------
        data : Dict[str, Any]
            Dataset to be saved .
        path: str
            Destination to save the yaml .

        Returns
        -------
        None

        """
        with Opener.open(path=path, mode="w") as fh:
            return safe_dump(data, fh, default_flow_style=False)
