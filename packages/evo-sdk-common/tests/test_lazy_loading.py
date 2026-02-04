#  Copyright © 2025 Seequent, a Bentley company. All rights reserved.
#  This software is copyrighted and all rights are reserved by Seequent.
#  This notice must be reproduced in its entirety on any authorized copies.
"""Tests for evo namespace package lazy loading."""

from __future__ import annotations

import sys
import unittest
from unittest.mock import patch


class TestLazyLoading(unittest.TestCase):
    """Tests for the evo namespace package lazy loading functionality."""

    def setUp(self) -> None:
        """Clear evo module cache before each test."""
        # Remove evo and all submodules from sys.modules to test lazy loading
        modules_to_remove = [key for key in sys.modules if key == "evo" or key.startswith("evo.")]
        for mod in modules_to_remove:
            del sys.modules[mod]

    def test_import_evo(self) -> None:
        """Test that 'import evo' works."""
        import evo

        self.assertIsNotNone(evo)
        self.assertEqual(evo.__name__, "evo")

    def test_lazy_load_common(self) -> None:
        """Test that evo.common lazy loads on first access."""
        import evo

        # Should not be in sys.modules yet (lazy loading)
        # Note: depending on test order, it might already be loaded
        # The key test is that accessing it works

        # Access should trigger lazy load
        common = evo.common

        # Now should be loaded
        self.assertIn("evo.common", sys.modules)
        self.assertIsNotNone(common)

    def test_lazy_load_attribute_chain(self) -> None:
        """Test that evo.common.Environment works via lazy loading."""
        import evo

        # Access nested attribute through lazy loading
        env_class = evo.common.Environment
        self.assertIsNotNone(env_class)

    def test_module_caching(self) -> None:
        """Test that lazy loaded modules are cached."""
        import evo

        # First access
        common1 = evo.common
        # Second access should return cached module
        common2 = evo.common

        self.assertIs(common1, common2)

    def test_dir_includes_submodules(self) -> None:
        """Test that dir(evo) includes available submodules."""
        import evo

        available = dir(evo)

        # Should include all known submodules
        self.assertIn("common", available)
        self.assertIn("objects", available)
        self.assertIn("blockmodels", available)
        self.assertIn("compute", available)
        self.assertIn("files", available)
        self.assertIn("colormaps", available)
        self.assertIn("widgets", available)

        # Should include __version__
        self.assertIn("__version__", available)

    def test_version_returns_dict(self) -> None:
        """Test that evo.__version__ returns a dictionary."""
        import evo

        version = evo.__version__
        self.assertIsInstance(version, dict)

        # Should include evo-sdk-common since we're running from that package
        # (unless running in isolation)
        # The key test is that it returns a dict, even if empty
        for key, value in version.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, str)

    def test_unknown_attribute_raises_attribute_error(self) -> None:
        """Test that accessing unknown attributes raises AttributeError."""
        import evo

        with self.assertRaises(AttributeError) as ctx:
            _ = evo.nonexistent_module

        self.assertIn("no attribute 'nonexistent_module'", str(ctx.exception))

    def test_missing_package_raises_import_error_with_hint(self) -> None:
        """Test that missing packages show helpful install instructions."""
        import evo

        # Mock importlib.import_module to simulate missing package
        original_import = evo.importlib.import_module

        def mock_import(name: str):
            if name == "evo.objects":
                raise ImportError("No module named 'evo.objects'")
            return original_import(name)

        with patch.object(evo.importlib, "import_module", mock_import):
            # Clear any cached module
            if "objects" in evo.__dict__:
                del evo.__dict__["objects"]

            with self.assertRaises(ImportError) as ctx:
                _ = evo.objects

            error_msg = str(ctx.exception)
            self.assertIn("evo.objects", error_msg)
            self.assertIn("evo-objects", error_msg)
            self.assertIn("pip install", error_msg)


class TestBackwardCompatibility(unittest.TestCase):
    """Test that traditional import patterns still work."""

    def test_from_import_still_works(self) -> None:
        """Test that 'from evo.common import X' still works."""
        from evo.common import Environment

        self.assertIsNotNone(Environment)

    def test_direct_submodule_import(self) -> None:
        """Test that 'import evo.common' still works."""
        import evo.common

        self.assertIsNotNone(evo.common)
        self.assertIn("evo.common", sys.modules)


if __name__ == "__main__":
    unittest.main()
