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

"""Modern anywidget-based Jupyter widgets for Seequent Evo SDK.

This package provides a modern implementation of Evo widgets using anywidget,
offering better compatibility across different Jupyter environments.

Example usage:
    ```python
    from evo.widgets import ServiceManagerWidget

    manager = await ServiceManagerWidget.with_auth_code(client_id="your-client-id").login()
    ```
"""

from evo.common.utils import split_feedback

from .widgets import (
    DropdownSelectorWidget,
    FeedbackWidget,
    HubSelectorWidget,
    ObjectSearchWidget,
    OrgSelectorWidget,
    ServiceManagerWidget,
    WorkspaceSelectorWidget,
    display_object_links,
)

__all__ = [
    "display_object_links",
    "DropdownSelectorWidget",
    "FeedbackWidget",
    "HubSelectorWidget",
    "ObjectSearchWidget",
    "OrgSelectorWidget",
    "ServiceManagerWidget",
    "split_feedback",
    "WorkspaceSelectorWidget",
]
