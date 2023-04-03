# -*- coding: utf-8 -*-
"""
Tests file_handler.py .
"""

from pathlib import Path
from typing import Callable
from unittest.mock import patch

import pytest


def test_read_dataset_csv(
    tmp_path: Path,
) -> None:
    """
    Tests read_dataset from class FileHandler with csv .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_csv = str(tmp_path / "dataset.csv")
    with open(dataset_path_csv, "w") as fh:
        iris = datasets.load_iris()
        dataset = DataFrame(
            data=c_[iris["data"], iris["target"]],
            columns=iris["feature_names"] + ["target"],
        )
        dataset.to_csv(path_or_buf=fh)

    # Test
    data_csv = FileHandler.read_dataset(path=dataset_path_csv)
    assert isinstance(data_csv, DataFrame)
    assert data_csv.shape[0] != 0
    assert data_csv.shape[1] != 0


def test_read_dataset_parquet(
    tmp_path: Path,
):
    """
    Tests read_dataset from class FileHandler with parquet .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_parquet = str(tmp_path / "dataset.parquet")
    with open(dataset_path_parquet, "wb") as fh:
        iris = datasets.load_iris()
        dataset = DataFrame(
            data=c_[iris["data"], iris["target"]],
            columns=iris["feature_names"] + ["target"],
        )
        dataset.to_parquet(fh)

    # Test
    data_parquet = FileHandler.read_dataset(path=dataset_path_parquet)
    assert isinstance(data_parquet, DataFrame)
    assert data_parquet.shape[0] != 0
    assert data_parquet.shape[1] != 0


@patch("s3fs.S3FileSystem.open", side_effect=open)
def test_read_dataset_cloud(
    mock_s3fs_open: Callable,
    tmp_path: Path,
):
    """
    Tests read_dataset from class FileHandler from cloud .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_csv = str("s3://" / tmp_path / "dataset.csv")
    with open(dataset_path_csv, "wb") as fh:
        iris = datasets.load_iris()
        dataset = DataFrame(
            data=c_[iris["data"], iris["target"]],
            columns=iris["feature_names"] + ["target"],
        )
        dataset.to_csv(path_or_buf=fh)

    # Test
    data_csv = FileHandler.read_dataset(path=dataset_path_csv)
    assert isinstance(data_csv, DataFrame)
    assert data_csv.shape[0] != 0
    assert data_csv.shape[1] != 0


def test_read_dataset_unknown_should_fail(
    tmp_path: Path,
):
    """
    Tests read_dataset from class FileHandler with unknown type.
    It should fail .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_unknown = str(tmp_path / "dataset.unknown")
    with open(dataset_path_unknown, "wb") as fh:
        iris = datasets.load_iris()
        dataset = DataFrame(
            data=c_[iris["data"], iris["target"]],
            columns=iris["feature_names"] + ["target"],
        )
        dataset.to_csv(path_or_buf=fh)

    # Test
    with pytest.raises(Exception):
        _ = FileHandler.read_dataset(path=dataset_path_unknown)


def test_save_dataset_csv(
    tmp_path: Path,
):
    """
    Tests save_dataset from class FileHandler with csv type .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame, read_csv
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_csv = str(tmp_path / "dataset.csv")
    iris = datasets.load_iris()
    dataset = DataFrame(
        data=c_[iris["data"], iris["target"]],
        columns=iris["feature_names"] + ["target"],
    )

    # Test
    FileHandler.save_dataset(dataset=dataset, path=dataset_path_csv)
    with open(dataset_path_csv, "rb") as fh:
        dataset = read_csv(fh)
    assert isinstance(dataset, DataFrame)
    assert dataset.shape[0] != 0
    assert dataset.shape[1] != 0


def test_save_dataset_parquet(
    tmp_path: Path,
):
    """
    Tests save_dataset from class FileHandler with parquet type .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame, read_parquet
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_parquet = str(tmp_path / "dataset.parquet")
    iris = datasets.load_iris()
    dataset = DataFrame(
        data=c_[iris["data"], iris["target"]],
        columns=iris["feature_names"] + ["target"],
    )

    # Test
    FileHandler.save_dataset(
        dataset=dataset, path=dataset_path_parquet, file_format="parquet"
    )
    with open(dataset_path_parquet, "rb") as fh:
        dataset = read_parquet(fh)
    assert isinstance(dataset, DataFrame)
    assert dataset.shape[0] != 0
    assert dataset.shape[1] != 0


def test_save_dataset_unknown_should_fail(
    tmp_path: Path,
):
    """
    Tests save_dataset from class FileHandler with unknown type.
    It should fail .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load dataset .

    Returns
    -------
    None

    """
    # Setup
    from numpy import c_
    from pandas import DataFrame
    from sklearn import datasets

    from file_handler import FileHandler

    dataset_path_unknown = str(tmp_path / "dataset.unknown")
    iris = datasets.load_iris()
    dataset = DataFrame(
        data=c_[iris["data"], iris["target"]],
        columns=iris["feature_names"] + ["target"],
    )

    # Test
    with pytest.raises(Exception):
        _ = FileHandler.save_dataset(
            dataset=dataset, path=dataset_path_unknown, file_format="unknown"
        )


def test_read_yaml(
    tmp_path: Path,
):
    """
    Tests read_yaml from class FileHandler .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load yaml .

    Returns
    -------
    None

    """
    # Setup
    from yaml import dump

    from file_handler import FileHandler

    data_path = str(tmp_path / "data.yaml")
    test_dict = {"test1": 1, "test2": "2"}
    with open(data_path, "w") as fh:
        dump(test_dict, fh)

    # Test
    dict_read = FileHandler.read_yaml(path=data_path)
    assert isinstance(dict_read, dict)
    assert "test1" in dict_read
    assert "test2" in dict_read
    assert dict_read["test1"] == 1
    assert dict_read["test2"] == "2"


@patch("s3fs.S3FileSystem.open", side_effect=open)
def test_read_yaml_cloud(
    mock_s3fs_open: Callable,
    tmp_path: Path,
):
    """
    Tests read_yaml from class FileHandler with cloud .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load data .

    Returns
    -------
    None

    """
    # Setup
    from yaml import dump

    from file_handler import FileHandler

    data_path = str(tmp_path / "data.yaml")
    test_dict = {"test1": 1, "test2": "2"}
    with open(data_path, "w") as fh:
        dump(test_dict, fh)

    # Test
    dict_read = FileHandler.read_yaml(path=data_path)
    assert isinstance(dict_read, dict)
    assert "test1" in dict_read
    assert "test2" in dict_read
    assert dict_read["test1"] == 1
    assert dict_read["test2"] == "2"


def test_save_yaml(
    tmp_path: Path,
):
    """
    Tests save_yaml from class FileHandler .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load yaml .

    Returns
    -------
    None

    """
    # Setup
    from yaml import safe_load

    from file_handler import FileHandler

    data_path = str(tmp_path / "data.yaml")
    test_dict = {"test1": 1, "test2": "2"}

    # Test
    dict_read = FileHandler.save_yaml(data=test_dict, path=data_path)
    with open(data_path, "r") as fh:
        dict_read = safe_load(fh)
    assert isinstance(dict_read, dict)
    assert "test1" in dict_read
    assert "test2" in dict_read
    assert dict_read["test1"] == 1
    assert dict_read["test2"] == "2"


@patch("s3fs.S3FileSystem.open", side_effect=open)
def test_save_yaml_cloud(
    mock_s3fs_open: Callable,
    tmp_path: Path,
):
    """
    Tests save_yaml from class FileHandler with cloud .

    Parameters
    ----------
    tmp_path: Path
        Artificial path to save and load yaml .

    Returns
    -------
    None

    """
    # Setup
    from yaml import safe_load

    from file_handler import FileHandler

    data_path = str(tmp_path / "data.yaml")
    test_dict = {"test1": 1, "test2": "2"}

    # Test
    dict_read = FileHandler.save_yaml(data=test_dict, path="s3://" + data_path)
    with open(data_path, "r") as fh:
        dict_read = safe_load(fh)
    assert isinstance(dict_read, dict)
    assert "test1" in dict_read
    assert "test2" in dict_read
    assert dict_read["test1"] == 1
    assert dict_read["test2"] == "2"
