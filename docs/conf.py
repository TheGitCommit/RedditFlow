# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
# CORRECT PATH: Points to C:\projects\reddit\src (parent of the 'reddit' package)
# This allows imports like 'import reddit.etl'
sys.path.insert(0, os.path.abspath('../..'))

project = 'RedditFlow'
project = 'RedditFlow'
copyright = '2025, Sean'
author = 'Sean'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",      # pull in docstrings
    "sphinx.ext.napoleon",     # support for Google/NumPy style docstrings
    "sphinx.ext.viewcode",     # link to source code
    "sphinx_autodoc_typehints" # optional, type hints in docs
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
