# -*- coding: utf-8 -*-
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

project = "File Handler"
author = "Felipe"

sys.path.insert(0, os.path.abspath("./file_handler"))

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # Include documentation from docstrings
    "sphinx.ext.autosummary",  # Generate autodoc summaries
    "sphinx.ext.doctest",  # Test snippets in the documentation
    "sphinx.ext.coverage",  # Collect doc coverage stats
    "sphinx.ext.viewcode",  # Add links to highlighted source code
    "sphinx.ext.autosectionlabel",  # Allow reference sections using its title
    "sphinx.ext.napoleon",  # Napoleon is a extension that enables Sphinx to parse both NumPy and Google style docstrings,
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "_templates", "_static", "__init__"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
