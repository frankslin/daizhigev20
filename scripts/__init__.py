"""
Scripts package for Chinese classics data processing.

This package contains utilities for importing and updating Chinese classical texts
in Elasticsearch indexes.
"""

from .import_classics import *

__all__ = [
    'connect_to_elasticsearch',
    'process_text_file',
    'process_markdown_file',
    'extract_markdown_metadata',
    'parse_markdown_file'
]