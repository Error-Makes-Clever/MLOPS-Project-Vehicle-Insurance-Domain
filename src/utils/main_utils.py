import os
import sys
import numpy as np
import dill
import yaml
from pandas import DataFrame

from src.exception import MyException
from src.logger import logging


# ------------------------------------------------------------
# YAML FILE UTILITIES
# ------------------------------------------------------------

def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML configuration file and returns its contents as a dictionary.
    
    Args:
        file_path (str): Path to the YAML file.

    Returns:
        dict: Parsed YAML content.
    """
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)

    except Exception as e:
        raise MyException(e, sys) from e


def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """
    Writes content to a YAML file.
    
    Args:
        file_path (str): Path where YAML file should be saved.
        content (object): Data to be written into the YAML file.
        replace (bool): If True, replaces existing file.
    """
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as file:
            yaml.dump(content, file)

    except Exception as e:
        raise MyException(e, sys) from e


# ------------------------------------------------------------
# MODEL / OBJECT SERIALIZATION UTILITIES
# ------------------------------------------------------------

def load_object(file_path: str) -> object:
    """
    Loads a serialized Python object using dill.

    Args:
        file_path (str): Path of the serialized file.

    Returns:
        object: Deserialized Python object.
    """
    try:
        with open(file_path, "rb") as file_obj:
            obj = dill.load(file_obj)
        return obj

    except Exception as e:
        raise MyException(e, sys) from e


def save_object(file_path: str, obj: object) -> None:
    """
    Saves any Python object to disk using dill.

    Args:
        file_path (str): Location where object will be stored.
        obj (object): Python object to serialize.
    """
    try:
        logging.info("Saving object to disk")

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)

        logging.info("Object saved successfully")

    except Exception as e:
        raise MyException(e, sys) from e


# ------------------------------------------------------------
# NUMPY ARRAY UTILITIES
# ------------------------------------------------------------

def save_numpy_array_data(file_path: str, array: np.ndarray):
    """
    Saves a NumPy array to disk in binary format (.npy).

    Args:
        file_path (str): Path to save the array.
        array (np.ndarray): NumPy array to be saved.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as file_obj:
            np.save(file_obj, array)

    except Exception as e:
        raise MyException(e, sys) from e


def load_numpy_array_data(file_path: str) -> np.ndarray:
    """
    Loads a NumPy array from disk.

    Args:
        file_path (str): Path to the saved NumPy file.

    Returns:
        np.ndarray: Loaded NumPy array.
    """
    try:
        with open(file_path, 'rb') as file_obj:
            return np.load(file_obj)

    except Exception as e:
        raise MyException(e, sys) from e