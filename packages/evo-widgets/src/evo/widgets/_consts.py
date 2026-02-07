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

"""Constants for evo.widgets package."""

DEFAULT_DISCOVERY_URL = "https://discover.api.seequent.com"
DEFAULT_BASE_URI = "https://ims.bentley.com"
DEFAULT_REDIRECT_URL = "http://localhost:3000/signin-callback"

DEFAULT_CACHE_LOCATION = "./notebook-data"

# Base64-encoded Evo logo (EvoBadgeCharcoal_FV.png)
EVO_LOGO_BASE64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAt4AAANJCAYAAAA7gDsQAAAACXBIWXMAAC4jAAAuIwF4pT92AAAgAElE"
    "QVR4nO3dz4tl6X3f8aNBMRFeTGcQyepyO1xj8GoqgeyEpq1/YNpZ5MfC6dZC4JDFtLGjJJu4R2A0hIT0"
    "LIQGGqMutEwC3dkkG2uqjPfuJhiBUJG+lL0RZjy10WwCE073c1vVXXWr7o/zfM/zPOf1gkbGiOnb55Zn"
    "3n7mc875ypdfftkBUL7ZfHHQdd2Naz7o89PlyXNfJ0B5hDdAIWbzRR/VB+nXzfSf/f/u3R0/4XH6z6M+"
    "yFOUH/m+AcYhvAFGkk6wb6Vf/f88D/okz1KMv/h1ujz53M8AQH7CGyDIbL64mSL7dvrPtwu59qsQf+xE"
    "HCAf4Q2QUYrtPrTv7jEZiXTWB3iK8Md+NgCGI7wBBlZhbK/TR/ij/tfp8uRpmR8RoB7CG2AA6cbIVWy/"
    "1+A1XXZd9yBFuE04wA6EN8AWzp1un2v8inJpo5QHwnMAAAcLSURBVIF+Apuoej8eN0k+cAEOsIvz4"
    "e3+ennL4rqusKenb59gu0RO+q4z7s+OKDn3Y+kXGe6srvR+6RQhCCHxBPfJE7bvn5CQJC3ZRLJ/WS"
)  # Truncated for readability - full base64 would be here

# Base64-encoded loading spinner (loading.gif)
LOADING_GIF_BASE64 = (
    "R0lGODlhuQEjAfQAAP///+fn587Ozr6+vrKyspqamo6OjoKCgnV1dWlpaVlZWVFRUUFBQT09PTk5OTU1"
    "Nf4BAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACH5BAQFAAAAI"
)  # Truncated for readability - full base64 would be here
