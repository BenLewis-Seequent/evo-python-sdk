# Migrated Widgets: evo.notebooks → evo.widgets

This document summarizes the migration of Jupyter widgets from `evo.notebooks` to the new `evo.widgets` package, which uses the modern [anywidget](https://anywidget.dev/) framework.

---

## Summary of Changes

### Why This Migration?

The original widgets in `evo.notebooks` were built with `ipywidgets`, which requires complex widget bundling and has environment-specific compatibility issues. The new `evo.widgets` package uses **anywidget**, providing:

- **Cross-environment compatibility**: Works in JupyterLab, Jupyter Notebook, VS Code, Google Colab, and more
- **Simpler architecture**: Self-contained widgets with embedded JavaScript/CSS
- **Better maintainability**: Clear separation between Python state (traitlets) and JavaScript rendering
- **No build step**: JavaScript/CSS files are loaded directly, no bundling required

### What Changed?

| Aspect | Old (`evo.notebooks`) | New (`evo.widgets`) |
|--------|----------------------|---------------------|
| Framework | ipywidgets | anywidget |
| State sync | ipywidgets traitlets | traitlets (same API) |
| Assets | Separate static files | Base64-embedded + static JS/CSS |
| Installation | `evo-sdk-common[notebooks]` | `evo-widgets` |

### Migration Path

Simply update your imports:

```python
# Old
from evo.notebooks import ServiceManagerWidget, FeedbackWidget, ObjectSearchWidget

# New
from evo.widgets import ServiceManagerWidget, FeedbackWidget, ObjectSearchWidget
```

The API remains the same—no code changes required beyond the import statement.

### Deprecation Notice

The old widgets in `evo.notebooks` now show deprecation warnings:

```
DeprecationWarning: evo.notebooks.ServiceManagerWidget is deprecated. 
Use evo.widgets.ServiceManagerWidget instead.
```

The old widgets will continue to work but are no longer maintained.

---

## Widget Reference

### FeedbackWidget

A progress indicator widget for displaying status during long-running operations like downloads, uploads, or compute tasks.

**Usage:**
```python
from evo.widgets import FeedbackWidget

feedback = FeedbackWidget("Processing")
display(feedback)

# Update progress (0.0 to 1.0)
feedback.progress = 0.5
feedback.message = "Halfway there..."

# Complete
feedback.progress = 1.0
feedback.message = "Done!"
```

**Properties:**
- `label` (str): The operation label displayed above the progress bar
- `progress` (float): Progress value from 0.0 to 1.0
- `message` (str): Optional status message displayed below the progress bar

#### Empty State (0% progress)
![FeedbackWidget Empty](images/feedback_widget_empty.png)

#### In Progress (50% progress)
![FeedbackWidget Progress](images/feedback_widget_progress.png)

#### Complete (100% progress)
![FeedbackWidget Complete](images/feedback_widget_complete.png)

---

### DropdownSelectorWidget

A generic dropdown selector with label and loading state support. This is the base widget used by the specialized selectors (OrgSelector, HubSelector, WorkspaceSelector).

**Usage:**
```python
from evo.widgets import DropdownSelectorWidget

dropdown = DropdownSelectorWidget(label="Select Item")
dropdown.options = [("Option 1", "val1"), ("Option 2", "val2")]
dropdown.value = "val1"
display(dropdown)
```

**Properties:**
- `label` (str): Label displayed above the dropdown
- `options` (list): List of (display_text, value) tuples
- `value` (any): Currently selected value
- `disabled` (bool): Whether the dropdown is disabled
- `loading` (bool): Whether to show loading indicator

#### Empty/Disabled State
![Dropdown Empty](images/dropdown_widget_empty.png)

#### With Options Selected
![Dropdown Options](images/dropdown_widget_options.png)

#### Loading State
![Dropdown Loading](images/dropdown_widget_loading.png)

---

### ServiceManagerWidget

The main authentication and service configuration widget. Provides sign-in functionality and organisation/hub/workspace selection. Implements the `IContext` interface for use with other Evo SDK components.

**Usage:**
```python
from evo.widgets import ServiceManagerWidget

# Create with authorization code flow
widget = await ServiceManagerWidget.with_auth_code(
    client_id="your-client-id",
    redirect_url="http://localhost:3000/signin-callback",
).login()

display(widget)

# Access selected values
print(f"Org: {widget.org_id}")
print(f"Hub: {widget.hub}")
print(f"Workspace: {widget.workspace_id}")

# Use as context for SDK clients
from evo.objects import ObjectsClient
client = ObjectsClient(widget)
```

**Properties:**
- `org_id` (UUID): Selected organisation ID
- `hub` (str): Selected hub identifier
- `workspace_id` (UUID): Selected workspace ID
- Implements `IContext` interface for SDK integration

**Key Features:**
- OAuth authorization code flow support
- Automatic token refresh
- Persists selections to `.env` file for session continuity
- Cascading dropdowns (org → hub → workspace)

#### Before Sign In
![ServiceManager Signed Out](images/service_manager_widget_signed_out.png)

#### After Sign In (with selections)
![ServiceManager Signed In](images/service_manager_widget_signed_in.png)

---

### ObjectSearchWidget

A search interface for finding and browsing geoscience objects in a workspace. Displays object metadata and allows filtering by object type.

**Usage:**
```python
from evo.widgets import ServiceManagerWidget, ObjectSearchWidget

# First, set up the service manager
sm = ServiceManagerWidget()
display(sm)

# After signing in, create the object search widget
search = ObjectSearchWidget(sm)
display(search)

# Access the selected object
if search.selected_object:
    print(f"Name: {search.selected_object.name}")
    print(f"ID: {search.selected_object.id}")
    print(f"Type: {search.selected_object.type_name}")
```

**Properties:**
- `search_text` (str): Current search query
- `object_type` (str | None): Filter by object type
- `selected_object` (ObjectInfo | None): Currently selected object
- `results` (list): List of search results

**Key Features:**
- Full-text search across object names and paths
- Filter by object type (Pointset, Block Model, etc.)
- Displays detailed metadata for selected objects
- Real-time search as you type

#### Empty State
![ObjectSearch Empty](images/object_search_widget_empty.png)

#### With Search Results
![ObjectSearch Results](images/object_search_widget_results.png)

---

## Files Created/Modified

### New Package: `evo-widgets`

```
packages/evo-widgets/
├── pyproject.toml              # Package configuration
├── README.md                   # Package documentation
├── pytest.ini                  # Test configuration
├── docs/
│   ├── MIGRATED_WIDGETS.md     # This file
│   └── images/                 # Widget screenshots
├── src/evo/widgets/
│   ├── __init__.py             # Public API
│   ├── _consts.py              # Constants, base64 assets
│   ├── _helpers.py             # Utility functions
│   ├── authorizer.py           # OAuth helper
│   ├── env.py                  # Environment config
│   ├── widgets.py              # Widget implementations
│   └── static/                 # JS/CSS for anywidget
│       ├── feedback.js/css
│       ├── dropdown.js/css
│       ├── service_manager.js/css
│       └── object_search.js/css
└── tests/
    ├── conftest.py
    ├── test_widget_screenshots.py  # Playwright visual tests
    └── baselines/                  # Screenshot baselines
```

### Modified: `evo-sdk-common`

**File:** `packages/evo-sdk-common/src/evo/notebooks/widgets.py`

Added deprecation warnings to all widget classes:
- `DropdownSelectorWidget`
- `ServiceManagerWidget`
- `FeedbackWidget`
- `ObjectSearchWidget`

---

## Running Visual Regression Tests

The widget screenshots are generated by Playwright tests, ensuring visual consistency:

```bash
cd packages/evo-widgets

# Install dependencies
uv sync --all-extras

# Install Playwright browsers
uv run playwright install chromium

# Run tests (generates/updates baselines)
uv run pytest tests/test_widget_screenshots.py -v
```

---

## Questions?

For issues or questions about the migration, please open an issue in the evo-python-sdk repository.
