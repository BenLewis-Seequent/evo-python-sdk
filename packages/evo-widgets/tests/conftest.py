#  Copyright © 2025 Bentley Systems, Incorporated
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Pytest configuration for evo-widgets tests."""

from __future__ import annotations

import pytest


def pytest_configure(config):
    """Add custom markers."""
    config.addinivalue_line(
        "markers", "visual: mark test as a visual regression test"
    )


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Configure browser context for consistent screenshots."""
    return {
        **browser_context_args,
        "viewport": {"width": 800, "height": 600},
        "device_scale_factor": 1,
    }
