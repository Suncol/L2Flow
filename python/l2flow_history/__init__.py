"""Repository-local Phase 8 historical-storage primitives.

The submodules expose versioned Parquet, manifest, query, retention, and
recovery building blocks.  Importing this package performs no filesystem I/O,
starts no service, and does not alter the production generation alias.
"""

__version__ = "0.1.0"
