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

from __future__ import annotations

import contextlib
import dataclasses
import uuid
from unittest.mock import patch

import numpy as np
import pandas as pd
from parameterized import parameterized

from evo.common import Environment, StaticContext
from evo.common.test_tools import BASE_URL, ORG, WORKSPACE_ID, TestWithConnector
from evo.objects import ObjectReference
from evo.objects.typed import DownholeCollection, DownholeCollectionData
from evo.objects.typed.base import BaseObject

from .helpers import MockClient


class TestDownholeCollection(TestWithConnector):
    def setUp(self) -> None:
        TestWithConnector.setUp(self)
        self.environment = Environment(hub_url=BASE_URL, org_id=ORG.id, workspace_id=WORKSPACE_ID)
        self.context = StaticContext.from_environment(
            environment=self.environment,
            connector=self.connector,
        )

    @contextlib.contextmanager
    def _mock_geoscience_objects(self):
        mock_client = MockClient(self.environment)
        with (
            patch("evo.objects.typed.dataset.get_data_client", lambda _: mock_client),
            patch("evo.objects.typed.base.create_geoscience_object", mock_client.create_geoscience_object),
            patch("evo.objects.typed.base.replace_geoscience_object", mock_client.replace_geoscience_object),
            patch("evo.objects.typed.base.download_geoscience_object", mock_client.from_reference),
        ):
            yield mock_client

    @property
    def example_downhole_collection(self) -> DownholeCollectionData:
        """Create an example downhole collection with 3 drill holes."""
        # Collar data with coordinates and hole IDs
        collars = pd.DataFrame(
            {
                "x": [100.0, 200.0, 300.0],
                "y": [1000.0, 1100.0, 1200.0],
                "z": [50.0, 55.0, 60.0],
                "hole_id": pd.Categorical(["DH-01", "DH-02", "DH-03"]),
            }
        )

        return DownholeCollectionData(
            name="Test Downhole Collection",
            collars=collars,
            distance_unit="m",
            desurvey="minimum_curvature",
        )

    @property
    def minimal_downhole_collection(self) -> DownholeCollectionData:
        """Create a minimal downhole collection with required fields only."""
        collars = pd.DataFrame(
            {
                "x": [100.0],
                "y": [1000.0],
                "z": [50.0],
                "hole_id": pd.Categorical(["DH-01"]),
            }
        )

        return DownholeCollectionData(
            name="Minimal Downhole Collection",
            collars=collars,
        )

    async def test_bounding_box_computation(self):
        """Test that bounding box is computed correctly from collar coordinates."""
        data = self.example_downhole_collection
        bbox = data.compute_bounding_box()

        self.assertIsNotNone(bbox)
        self.assertEqual(bbox.min_x, 100.0)
        self.assertEqual(bbox.max_x, 300.0)
        self.assertEqual(bbox.min_y, 1000.0)
        self.assertEqual(bbox.max_y, 1200.0)
        self.assertEqual(bbox.min_z, 50.0)
        self.assertEqual(bbox.max_z, 60.0)

    async def test_bounding_box_empty(self):
        """Test bounding box with empty coordinates."""
        data = dataclasses.replace(
            self.minimal_downhole_collection,
            collars=pd.DataFrame({"x": [], "y": [], "z": [], "hole_id": pd.Categorical([])}),
        )
        bbox = data.compute_bounding_box()
        self.assertIsNone(bbox)

    @parameterized.expand([BaseObject, DownholeCollection])
    async def test_create(self, class_to_call):
        """Test creating a downhole collection."""
        with self._mock_geoscience_objects():
            result = await class_to_call.create(context=self.context, data=self.example_downhole_collection)

        self.assertIsInstance(result, DownholeCollection)
        self.assertEqual(result.name, "Test Downhole Collection")
        self.assertEqual(result.distance_unit, "m")
        self.assertEqual(result.desurvey, "minimum_curvature")

    @parameterized.expand([BaseObject, DownholeCollection])
    async def test_create_minimal(self, class_to_call):
        """Test creating a minimal downhole collection with only required fields."""
        with self._mock_geoscience_objects():
            result = await class_to_call.create(context=self.context, data=self.minimal_downhole_collection)

        self.assertIsInstance(result, DownholeCollection)
        self.assertEqual(result.name, "Minimal Downhole Collection")
        self.assertIsNone(result.distance_unit)
        self.assertIsNone(result.desurvey)

    @parameterized.expand([BaseObject, DownholeCollection])
    async def test_replace(self, class_to_call):
        """Test replacing a downhole collection."""
        data = dataclasses.replace(
            self.example_downhole_collection,
            name="Replaced Downhole Collection",
        )

        with self._mock_geoscience_objects():
            result = await class_to_call.replace(
                context=self.context,
                reference=ObjectReference.new(
                    environment=self.context.get_environment(),
                    object_id=uuid.uuid4(),
                ),
                data=data,
            )

        self.assertIsInstance(result, DownholeCollection)
        self.assertEqual(result.name, "Replaced Downhole Collection")

    @parameterized.expand([BaseObject, DownholeCollection])
    async def test_create_or_replace(self, class_to_call):
        """Test create or replace a downhole collection."""
        with self._mock_geoscience_objects():
            result = await class_to_call.create_or_replace(
                context=self.context,
                reference=ObjectReference.new(
                    environment=self.context.get_environment(),
                    object_id=uuid.uuid4(),
                ),
                data=self.example_downhole_collection,
            )

        self.assertIsInstance(result, DownholeCollection)
        self.assertEqual(result.name, "Test Downhole Collection")

    async def test_from_reference(self):
        """Test loading a downhole collection from a reference."""
        with self._mock_geoscience_objects():
            original = await DownholeCollection.create(context=self.context, data=self.example_downhole_collection)

            result = await DownholeCollection.from_reference(context=self.context, reference=original.metadata.url)

        self.assertIsInstance(result, DownholeCollection)
        self.assertEqual(result.name, "Test Downhole Collection")

    async def test_update(self):
        """Test updating a downhole collection."""
        with self._mock_geoscience_objects():
            obj = await DownholeCollection.create(context=self.context, data=self.example_downhole_collection)

            self.assertEqual(obj.metadata.version_id, "1")
            obj.name = "Updated Downhole Collection"
            obj.distance_unit = "ft"

            await obj.update()

            self.assertEqual(obj.metadata.version_id, "2")
            self.assertEqual(obj.name, "Updated Downhole Collection")
            self.assertEqual(obj.distance_unit, "ft")

    async def test_multiple_holes(self):
        """Test downhole collection with multiple drill holes."""
        # Create a larger dataset with 10 holes
        num_holes = 10
        collars = pd.DataFrame(
            {
                "x": np.random.uniform(0, 1000, num_holes),
                "y": np.random.uniform(0, 1000, num_holes),
                "z": np.random.uniform(0, 100, num_holes),
                "hole_id": pd.Categorical([f"DH-{i:02d}" for i in range(1, num_holes + 1)]),
            }
        )

        data = DownholeCollectionData(
            name="Large Downhole Collection",
            collars=collars,
            distance_unit="m",
        )

        with self._mock_geoscience_objects():
            result = await DownholeCollection.create(context=self.context, data=data)

        self.assertIsInstance(result, DownholeCollection)
        self.assertEqual(result.name, "Large Downhole Collection")
