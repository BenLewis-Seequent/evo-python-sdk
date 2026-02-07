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

"""Visual regression tests for evo.widgets using Playwright.

This module provides visual regression testing for the anywidget-based widgets.
It renders widgets in a headless browser and compares screenshots against baselines.

Usage:
    # Run tests (will fail if baselines don't exist)
    uv run pytest tests/test_widget_screenshots.py

    # Update baselines
    uv run pytest tests/test_widget_screenshots.py --update-snapshots

Requirements:
    - pytest-playwright
    - nbconvert
    - playwright browsers installed: `playwright install chromium`
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

# Check if playwright is available
try:
    from playwright.sync_api import Page, expect
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


BASELINE_DIR = Path(__file__).parent / "baselines"
BASELINE_DIR.mkdir(exist_ok=True)


def create_widget_html(widget_name: str, widget_config: dict, static_base: str) -> str:
    """Create a minimal HTML page that renders a widget for screenshot testing.

    :param widget_name: Name of the widget class
    :param widget_config: Configuration dict with widget traits
    :param static_base: Base URL for static files
    :return: HTML content string
    """
    # Convert config to JSON for JavaScript
    config_json = json.dumps(widget_config)

    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{widget_name} Test</title>
    <link rel="stylesheet" href="{static_base}/static/{widget_name.lower()}.css">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            padding: 20px;
            background: #fff;
            margin: 0;
        }}
        #widget-container {{
            padding: 10px;
            display: inline-block;
            min-width: 400px;
        }}
    </style>
</head>
<body>
    <div id="widget-container">
        <div id="widget-root"></div>
    </div>
    <script type="module">
        // Mock model for testing
        class MockModel {{
            constructor(config) {{
                this._data = config;
                this._listeners = {{}};
            }}
            get(key) {{
                return this._data[key];
            }}
            set(key, value) {{
                this._data[key] = value;
            }}
            save_changes() {{}}
            on(event, callback) {{
                if (!this._listeners[event]) {{
                    this._listeners[event] = [];
                }}
                this._listeners[event].push(callback);
            }}
            off(event, callback) {{
                if (this._listeners[event]) {{
                    this._listeners[event] = this._listeners[event].filter(cb => cb !== callback);
                }}
            }}
        }}

        // Import and render widget
        const config = {config_json};
        const model = new MockModel(config);
        const el = document.getElementById('widget-root');

        // Dynamic import based on widget type
        const widgetModule = await import('{static_base}/static/{widget_name.lower()}.js');
        widgetModule.default.render({{ model, el }});

        // Signal that widget is ready
        window.widgetReady = true;
    </script>
</body>
</html>
"""


@pytest.fixture(scope="session")
def server_info(tmp_path_factory):
    """Create a simple static file server for widget JS/CSS files and return server info."""
    import http.server
    import socketserver
    import threading

    tmp_path = tmp_path_factory.mktemp("static_server")

    # Copy static files to temp directory
    static_src = Path(__file__).parent.parent / "src" / "evo" / "widgets" / "static"
    static_dest = tmp_path / "static"
    static_dest.mkdir()

    for f in static_src.glob("*"):
        (static_dest / f.name).write_text(f.read_text())

    # Start server with port 0 to let OS pick an available port
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(tmp_path), **kwargs)

        def log_message(self, format, *args):
            pass  # Suppress logging

    httpd = socketserver.TCPServer(("", 0), Handler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield {"url": f"http://localhost:{port}", "path": tmp_path}
    httpd.shutdown()


@pytest.mark.skipif(not PLAYWRIGHT_AVAILABLE, reason="playwright not installed")
class TestWidgetScreenshots:
    """Visual regression tests for widgets."""

    def test_feedback_widget_empty(self, page: Page, server_info: dict):
        """Test FeedbackWidget with 0% progress."""
        html_content = create_widget_html("feedback", {
            "label": "Processing",
            "progress": 0.0,
            "message": "",
        }, server_info["url"])

        html_file = server_info["path"] / "feedback_empty.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/feedback_empty.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "feedback_widget_empty.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_feedback_widget_progress(self, page: Page, server_info: dict):
        """Test FeedbackWidget with 50% progress and message."""
        html_content = create_widget_html("feedback", {
            "label": "Downloading",
            "progress": 0.5,
            "message": "50% complete...",
        }, server_info["url"])

        html_file = server_info["path"] / "feedback_progress.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/feedback_progress.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "feedback_widget_progress.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_feedback_widget_complete(self, page: Page, server_info: dict):
        """Test FeedbackWidget with 100% progress."""
        html_content = create_widget_html("feedback", {
            "label": "Upload",
            "progress": 1.0,
            "message": "Complete!",
        }, server_info["url"])

        html_file = server_info["path"] / "feedback_complete.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/feedback_complete.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "feedback_widget_complete.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_dropdown_widget_empty(self, page: Page, server_info: dict):
        """Test DropdownSelectorWidget with no selection."""
        html_content = create_widget_html("dropdown", {
            "label": "Organisation",
            "options": [["Select Organisation", None]],
            "value": None,
            "disabled": True,
            "loading": False,
        }, server_info["url"])

        html_file = server_info["path"] / "dropdown_empty.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/dropdown_empty.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "dropdown_widget_empty.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_dropdown_widget_with_options(self, page: Page, server_info: dict):
        """Test DropdownSelectorWidget with options."""
        html_content = create_widget_html("dropdown", {
            "label": "Hub",
            "options": [
                ["Select Hub", ""],
                ["Production Hub", "prod"],
                ["Development Hub", "dev"],
            ],
            "value": "prod",
            "disabled": False,
            "loading": False,
        }, server_info["url"])

        html_file = server_info["path"] / "dropdown_options.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/dropdown_options.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "dropdown_widget_options.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_dropdown_widget_loading(self, page: Page, server_info: dict):
        """Test DropdownSelectorWidget in loading state."""
        html_content = create_widget_html("dropdown", {
            "label": "Workspace",
            "options": [["Loading...", None]],
            "value": None,
            "disabled": True,
            "loading": True,
        }, server_info["url"])

        html_file = server_info["path"] / "dropdown_loading.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/dropdown_loading.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "dropdown_widget_loading.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_service_manager_widget_signed_out(self, page: Page, server_info: dict):
        """Test ServiceManagerWidget before sign in."""
        html_content = create_widget_html("service_manager", {
            "button_text": "Sign In",
            "button_disabled": False,
            "button_clicked": False,
            "main_loading": False,
            "prompt_text": "",
            "show_prompt": False,
            "org_options": [["Select Organisation", "00000000-0000-0000-0000-000000000000"]],
            "org_value": "00000000-0000-0000-0000-000000000000",
            "org_loading": False,
            "hub_options": [["Select Hub", ""]],
            "hub_value": "",
            "hub_loading": False,
            "ws_options": [["Select Workspace", "00000000-0000-0000-0000-000000000000"]],
            "ws_value": "00000000-0000-0000-0000-000000000000",
            "ws_loading": False,
        }, server_info["url"])

        html_file = server_info["path"] / "service_manager_signed_out.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/service_manager_signed_out.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "service_manager_widget_signed_out.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_service_manager_widget_signed_in(self, page: Page, server_info: dict):
        """Test ServiceManagerWidget after sign in with selections."""
        html_content = create_widget_html("service_manager", {
            "button_text": "Refresh Evo Services",
            "button_disabled": False,
            "button_clicked": False,
            "main_loading": False,
            "prompt_text": "",
            "show_prompt": False,
            "org_options": [
                ["Select Organisation", "00000000-0000-0000-0000-000000000000"],
                ["Seequent", "11111111-1111-1111-1111-111111111111"],
                ["Demo Org", "22222222-2222-2222-2222-222222222222"],
            ],
            "org_value": "11111111-1111-1111-1111-111111111111",
            "org_loading": False,
            "hub_options": [
                ["Select Hub", ""],
                ["Production", "prod"],
                ["Integration", "int"],
            ],
            "hub_value": "prod",
            "hub_loading": False,
            "ws_options": [
                ["Select Workspace", "00000000-0000-0000-0000-000000000000"],
                ["My Workspace", "33333333-3333-3333-3333-333333333333"],
                ["Demo Project", "44444444-4444-4444-4444-444444444444"],
            ],
            "ws_value": "33333333-3333-3333-3333-333333333333",
            "ws_loading": False,
        }, server_info["url"])

        html_file = server_info["path"] / "service_manager_signed_in.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/service_manager_signed_in.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "service_manager_widget_signed_in.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_object_search_widget_empty(self, page: Page, server_info: dict):
        """Test ObjectSearchWidget with no results."""
        html_content = create_widget_html("object_search", {
            "search_text": "",
            "object_type": None,
            "type_options": [
                ["All types", None],
                ["Pointset", "pointset"],
                ["Block Model", "block-model"],
            ],
            "result_options": [["No results", None]],
            "selected_id": None,
            "loading": False,
            "status_text": "",
            "metadata_text": "",
        }, server_info["url"])

        html_file = server_info["path"] / "object_search_empty.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/object_search_empty.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "object_search_widget_empty.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))

    def test_object_search_widget_with_results(self, page: Page, server_info: dict):
        """Test ObjectSearchWidget with search results."""
        html_content = create_widget_html("object_search", {
            "search_text": "Ag",
            "object_type": "pointset",
            "type_options": [
                ["All types", None],
                ["Pointset", "pointset"],
                ["Block Model", "block-model"],
            ],
            "result_options": [
                ["Ag_Assays (Pointset)", "11111111-1111-1111-1111-111111111111"],
                ["Ag_Samples (Pointset)", "22222222-2222-2222-2222-222222222222"],
            ],
            "selected_id": "11111111-1111-1111-1111-111111111111",
            "loading": False,
            "status_text": "Found 2 object(s)",
            "metadata_text": "============================================================\\n📦 Ag_Assays\\n============================================================\\nType:        Pointset\\nPath:        /data/assays/Ag_Assays\\nObject ID:   11111111-1111-1111-1111-111111111111\\n\\nCreated:     2025-01-15 10:30:00\\nModified:    2025-01-20 14:45:00",
        }, server_info["url"])

        html_file = server_info["path"] / "object_search_results.html"
        html_file.write_text(html_content)

        page.goto(f"{server_info['url']}/object_search_results.html")
        page.wait_for_function("window.widgetReady === true", timeout=5000)

        screenshot_path = BASELINE_DIR / "object_search_widget_results.png"
        page.locator("#widget-container").screenshot(path=str(screenshot_path))
