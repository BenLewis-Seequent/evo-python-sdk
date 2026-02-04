#  Copyright © 2025 Seequent, a Bentley company. All rights reserved.
#  This software is copyrighted and all rights are reserved by Seequent.
#  This notice must be reproduced in its entirety on any authorized copies.
"""Seequent Evo Python SDK namespace package with lazy loading.

This enables usage like:
    import evo
    evo.common.Environment  # Lazy loads evo.common
    evo.objects.PointSet    # Lazy loads evo.objects

Example:
    >>> import evo
    >>> print(evo.__version__)
    {'evo-sdk-common': '0.5.12', 'evo-objects': '0.3.3', ...}
"""

from __future__ import annotations

import importlib
from pkgutil import extend_path
from typing import TYPE_CHECKING, Any

# Extend path for editable installs where packages live in different directories
__path__ = extend_path(__path__, __name__)

# Mapping of submodule names to their package names for helpful error messages
_SUBMODULES: dict[str, str] = {
    # evo-sdk-common
    "common": "evo-sdk-common",
    "notebooks": "evo-sdk-common",
    "logging": "evo-sdk-common",
    # evo-objects
    "objects": "evo-objects",
    # evo-blockmodels
    "blockmodels": "evo-blockmodels",
    # evo-compute
    "compute": "evo-compute",
    # evo-files
    "files": "evo-files",
    # evo-colormaps
    "colormaps": "evo-colormaps",
}

# All known package names for version detection
_PACKAGES: list[str] = [
    "evo-sdk-common",
    "evo-objects",
    "evo-blockmodels",
    "evo-compute",
    "evo-files",
    "evo-colormaps",
]


def _get_version() -> dict[str, str]:
    """Get versions of all installed evo packages.

    Returns:
        Dictionary mapping package names to their installed versions.
        Only includes packages that are actually installed.
    """
    from importlib.metadata import PackageNotFoundError, version

    versions: dict[str, str] = {}
    for package in _PACKAGES:
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            pass  # Package not installed, skip it
    return versions


def __getattr__(name: str) -> Any:
    """Lazy load submodules on first access.

    This allows `evo.common.Environment` to work without explicit imports.
    Modules are cached after first access for performance.

    Args:
        name: The attribute name being accessed.

    Returns:
        The requested module or attribute.

    Raises:
        ImportError: If the submodule's package is not installed.
        AttributeError: If the attribute is not a known submodule.
    """
    if name == "__version__":
        return _get_version()

    if name in _SUBMODULES:
        try:
            module = importlib.import_module(f"evo.{name}")
            # Cache in module namespace for future access
            globals()[name] = module
            return module
        except ImportError as e:
            package_name = _SUBMODULES[name]
            raise ImportError(
                f"The 'evo.{name}' module requires the '{package_name}' package. "
                f"Install it with: pip install {package_name}"
            ) from e

    raise AttributeError(f"module 'evo' has no attribute '{name}'")


def __dir__() -> list[str]:
    """List available submodules and attributes for auto-completion.

    Only includes submodules that are actually installed, using find_spec()
    to check without importing (preserves lazy loading).

    Returns:
        Sorted list of available attribute names.
    """
    import importlib.util

    attrs = ["__version__", "__path__", "__name__", "__doc__", "__file__"]
    for name in _SUBMODULES:
        if importlib.util.find_spec(f"evo.{name}") is not None:
            attrs.append(name)
    return sorted(set(attrs))


if TYPE_CHECKING:
    # For static type checkers, provide explicit imports
    from evo import blockmodels as blockmodels
    from evo import colormaps as colormaps
    from evo import common as common
    from evo import compute as compute
    from evo import files as files
    from evo import logging as logging
    from evo import notebooks as notebooks
    from evo import objects as objects

    __version__: dict[str, str]
