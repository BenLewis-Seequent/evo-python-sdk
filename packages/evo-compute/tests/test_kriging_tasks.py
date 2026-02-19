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

"""Tests for kriging task parameter handling."""

from unittest import TestCase
from unittest.mock import MagicMock

from evo.compute.tasks import (
    CreateAttribute,
    RegionFilter,
    SearchNeighborhood,
    Source,
    Target,
    UpdateAttribute,
)
from evo.compute.tasks.common import Ellipsoid, EllipsoidRanges
from evo.compute.tasks.kriging import KrigingParameters


class TestKrigingParametersWithAttributes(TestCase):
    """Tests for KrigingParameters handling of typed attribute objects."""

    def _create_mock_attribute(self, name: str, exists: bool, object_url: str) -> MagicMock:
        """Create a mock attribute that behaves like Attribute or PendingAttribute."""
        attr = MagicMock()
        attr.name = name
        attr.exists = exists
        attr.expression = f"locations.attributes[?name=='{name}']"

        # Mock the _obj for object URL access
        mock_obj = MagicMock()
        mock_obj.metadata.url = object_url
        attr._obj = mock_obj

        if exists:
            attr.to_target_dict.return_value = {
                "operation": "update",
                "reference": attr.expression,
            }
        else:
            attr.to_target_dict.return_value = {
                "operation": "create",
                "name": name,
            }

        return attr

    def _create_mock_block_model_attribute(self, name: str, exists: bool, object_url: str) -> MagicMock:
        """Create a mock BlockModelAttribute or BlockModelPendingAttribute."""
        attr = MagicMock()
        attr.name = name
        attr.exists = exists
        attr.expression = f"attributes[?name=='{name}']"

        # Mock the _obj for object URL access (unified interface with Attribute)
        mock_bm = MagicMock()
        mock_bm.metadata.url = object_url
        attr._obj = mock_bm

        if exists:
            attr.to_target_dict.return_value = {
                "operation": "update",
                "reference": attr.expression,
            }
        else:
            attr.to_target_dict.return_value = {
                "operation": "create",
                "name": name,
            }

        return attr

    def test_kriging_params_with_pending_attribute_target(self):
        """Test KrigingParameters accepts PendingAttribute as target."""
        source = Source(object="https://example.com/pointset", attribute="locations.attributes[?name=='grade']")
        target_attr = self._create_mock_attribute(
            name="kriged_grade",
            exists=False,
            object_url="https://example.com/grid",
        )
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source,
            target=target_attr,
            variogram=variogram,
            search=search,
        )

        # Verify the target was converted correctly
        params_dict = params.to_dict()
        self.assertEqual(params_dict["target"]["object"], "https://example.com/grid")
        self.assertEqual(params_dict["target"]["attribute"]["operation"], "create")
        self.assertEqual(params_dict["target"]["attribute"]["name"], "kriged_grade")

    def test_kriging_params_with_existing_attribute_target(self):
        """Test KrigingParameters accepts existing Attribute as target."""
        source = Source(object="https://example.com/pointset", attribute="locations.attributes[?name=='grade']")
        target_attr = self._create_mock_attribute(
            name="existing_attr",
            exists=True,
            object_url="https://example.com/grid",
        )
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source,
            target=target_attr,
            variogram=variogram,
            search=search,
        )

        # Verify the target was converted correctly
        params_dict = params.to_dict()
        self.assertEqual(params_dict["target"]["object"], "https://example.com/grid")
        self.assertEqual(params_dict["target"]["attribute"]["operation"], "update")
        self.assertIn("reference", params_dict["target"]["attribute"])

    def test_kriging_params_with_block_model_pending_attribute(self):
        """Test KrigingParameters accepts BlockModelPendingAttribute as target."""
        source = Source(object="https://example.com/pointset", attribute="locations.attributes[?name=='grade']")
        target_attr = self._create_mock_block_model_attribute(
            name="new_bm_attr",
            exists=False,
            object_url="https://example.com/blockmodel",
        )
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source,
            target=target_attr,
            variogram=variogram,
            search=search,
        )

        # Verify the target was converted correctly
        params_dict = params.to_dict()
        self.assertEqual(params_dict["target"]["object"], "https://example.com/blockmodel")
        self.assertEqual(params_dict["target"]["attribute"]["operation"], "create")
        self.assertEqual(params_dict["target"]["attribute"]["name"], "new_bm_attr")

    def test_kriging_params_with_block_model_existing_attribute(self):
        """Test KrigingParameters accepts existing BlockModelAttribute as target."""
        source = Source(object="https://example.com/pointset", attribute="locations.attributes[?name=='grade']")
        target_attr = self._create_mock_block_model_attribute(
            name="existing_bm_attr",
            exists=True,
            object_url="https://example.com/blockmodel",
        )
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source,
            target=target_attr,
            variogram=variogram,
            search=search,
        )

        # Verify the target was converted correctly
        params_dict = params.to_dict()
        self.assertEqual(params_dict["target"]["object"], "https://example.com/blockmodel")
        self.assertEqual(params_dict["target"]["attribute"]["operation"], "update")
        self.assertIn("reference", params_dict["target"]["attribute"])

    def test_kriging_params_with_explicit_target(self):
        """Test KrigingParameters still works with explicit Target object."""
        source = Source(object="https://example.com/pointset", attribute="locations.attributes[?name=='grade']")
        target = Target.new_attribute("https://example.com/grid", "kriged_grade")
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source,
            target=target,
            variogram=variogram,
            search=search,
        )

        # Verify the target works correctly
        params_dict = params.to_dict()
        self.assertEqual(params_dict["target"]["object"], "https://example.com/grid")
        self.assertEqual(params_dict["target"]["attribute"]["operation"], "create")
        self.assertEqual(params_dict["target"]["attribute"]["name"], "kriged_grade")

    def test_kriging_params_source_attribute_conversion(self):
        """Test KrigingParameters converts source Attribute correctly."""
        # Create mock source attribute
        source_attr = MagicMock()
        source_attr.expression = "locations.attributes[?key=='grade']"
        mock_obj = MagicMock()
        mock_obj.metadata.url = "https://example.com/pointset"
        source_attr._obj = mock_obj
        # Mock to_source_dict method (the new pattern for Attribute)
        source_attr.to_source_dict.return_value = {
            "object": "https://example.com/pointset",
            "attribute": "locations.attributes[?key=='grade']",
        }

        target = Target.new_attribute("https://example.com/grid", "kriged_grade")
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source_attr,
            target=target,
            variogram=variogram,
            search=search,
        )

        # Verify the source was converted correctly
        params_dict = params.to_dict()
        self.assertEqual(params_dict["source"]["object"], "https://example.com/pointset")
        self.assertEqual(params_dict["source"]["attribute"], "locations.attributes[?key=='grade']")


class TestTargetSerialization(TestCase):
    """Tests for Target serialization with different attribute types."""

    def test_target_with_create_attribute(self):
        """Test Target serializes CreateAttribute correctly."""
        target = Target(
            object="https://example.com/grid",
            attribute=CreateAttribute(name="new_attr"),
        )

        result = target.to_dict()

        self.assertEqual(result["object"], "https://example.com/grid")
        self.assertEqual(result["attribute"]["operation"], "create")
        self.assertEqual(result["attribute"]["name"], "new_attr")

    def test_target_with_update_attribute(self):
        """Test Target serializes UpdateAttribute correctly."""
        target = Target(
            object="https://example.com/grid",
            attribute=UpdateAttribute(reference="cell_attributes[?name=='existing']"),
        )

        result = target.to_dict()

        self.assertEqual(result["object"], "https://example.com/grid")
        self.assertEqual(result["attribute"]["operation"], "update")
        self.assertEqual(result["attribute"]["reference"], "cell_attributes[?name=='existing']")

    def test_target_with_dict_attribute(self):
        """Test Target serializes dict attribute correctly."""
        target = Target(
            object="https://example.com/grid",
            attribute={"operation": "create", "name": "dict_attr"},
        )

        result = target.to_dict()

        self.assertEqual(result["object"], "https://example.com/grid")
        self.assertEqual(result["attribute"]["operation"], "create")
        self.assertEqual(result["attribute"]["name"], "dict_attr")

    def test_target_new_attribute_factory(self):
        """Test Target.new_attribute factory method."""
        target = Target.new_attribute("https://example.com/grid", "new_attr")

        result = target.to_dict()

        self.assertEqual(result["object"], "https://example.com/grid")
        self.assertEqual(result["attribute"]["operation"], "create")
        self.assertEqual(result["attribute"]["name"], "new_attr")


class TestRegionFilter(TestCase):
    """Tests for RegionFilter class."""

    def test_region_filter_with_names(self):
        """Test RegionFilter with category names."""
        region_filter = RegionFilter(
            attribute="domain_attribute",
            names=["LMS1", "LMS2"],
        )

        result = region_filter.to_dict()

        self.assertEqual(result["attribute"], "domain_attribute")
        self.assertEqual(result["names"], ["LMS1", "LMS2"])
        self.assertNotIn("values", result)

    def test_region_filter_with_values(self):
        """Test RegionFilter with integer values."""
        region_filter = RegionFilter(
            attribute="domain_code_attribute",
            values=[1, 2, 3],
        )

        result = region_filter.to_dict()

        self.assertEqual(result["attribute"], "domain_code_attribute")
        self.assertEqual(result["values"], [1, 2, 3])
        self.assertNotIn("names", result)

    def test_region_filter_with_block_model_attribute(self):
        """Test RegionFilter with BlockModelAttribute-like object."""
        mock_attr = MagicMock()
        mock_attr.expression = "attributes[?name=='domain']"

        region_filter = RegionFilter(
            attribute=mock_attr,
            names=["Zone1"],
        )

        result = region_filter.to_dict()

        self.assertEqual(result["attribute"], "attributes[?name=='domain']")
        self.assertEqual(result["names"], ["Zone1"])

    def test_region_filter_with_pointset_attribute(self):
        """Test RegionFilter with PointSet attribute-like object."""
        mock_attr = MagicMock()
        mock_attr.to_source_dict.return_value = {
            "object": "https://example.com/pointset",
            "attribute": "locations.attributes[?name=='domain']",
        }
        # Remove expression attribute so it falls through to to_source_dict
        del mock_attr.expression

        region_filter = RegionFilter(
            attribute=mock_attr,
            names=["Domain1"],
        )

        result = region_filter.to_dict()

        self.assertEqual(result["attribute"], "locations.attributes[?name=='domain']")
        self.assertEqual(result["names"], ["Domain1"])

    def test_region_filter_cannot_have_both_names_and_values(self):
        """Test RegionFilter raises error when both names and values are provided."""
        with self.assertRaises(ValueError) as context:
            RegionFilter(
                attribute="domain_attribute",
                names=["LMS1"],
                values=[1],
            )

        self.assertIn("Only one of 'names' or 'values' may be provided", str(context.exception))

    def test_region_filter_must_have_names_or_values(self):
        """Test RegionFilter raises error when neither names nor values are provided."""
        with self.assertRaises(ValueError) as context:
            RegionFilter(
                attribute="domain_attribute",
            )

        self.assertIn("One of 'names' or 'values' must be provided", str(context.exception))


class TestKrigingParametersWithRegionFilter(TestCase):
    """Tests for KrigingParameters with target region filter support."""

    def test_kriging_params_with_target_region_filter_names(self):
        """Test KrigingParameters with target region filter using category names."""
        source = Source(object="https://example.com/pointset", attribute="grade")
        target = Target.new_attribute("https://example.com/grid", "kriged_grade")
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )
        region_filter = RegionFilter(
            attribute="domain_attribute",
            names=["LMS1", "LMS2"],
        )

        params = KrigingParameters(
            source=source,
            target=target,
            variogram=variogram,
            search=search,
            target_region_filter=region_filter,
        )

        params_dict = params.to_dict()

        # Verify region filter is in target
        self.assertIn("region_filter", params_dict["target"])
        self.assertEqual(params_dict["target"]["region_filter"]["attribute"], "domain_attribute")
        self.assertEqual(params_dict["target"]["region_filter"]["names"], ["LMS1", "LMS2"])

    def test_kriging_params_with_target_region_filter_values(self):
        """Test KrigingParameters with target region filter using integer values."""
        source = Source(object="https://example.com/pointset", attribute="grade")
        target = Target.new_attribute("https://example.com/grid", "kriged_grade")
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )
        region_filter = RegionFilter(
            attribute="domain_code",
            values=[1, 2, 3],
        )

        params = KrigingParameters(
            source=source,
            target=target,
            variogram=variogram,
            search=search,
            target_region_filter=region_filter,
        )

        params_dict = params.to_dict()

        # Verify region filter is in target
        self.assertIn("region_filter", params_dict["target"])
        self.assertEqual(params_dict["target"]["region_filter"]["attribute"], "domain_code")
        self.assertEqual(params_dict["target"]["region_filter"]["values"], [1, 2, 3])

    def test_kriging_params_without_target_region_filter(self):
        """Test KrigingParameters without target region filter (default behavior)."""
        source = Source(object="https://example.com/pointset", attribute="grade")
        target = Target.new_attribute("https://example.com/grid", "kriged_grade")
        variogram = "https://example.com/variogram"
        search = SearchNeighborhood(
            ellipsoid=Ellipsoid(ranges=EllipsoidRanges(100, 100, 50)),
            max_samples=20,
        )

        params = KrigingParameters(
            source=source,
            target=target,
            variogram=variogram,
            search=search,
        )

        params_dict = params.to_dict()

        # Verify region filter is not present
        self.assertNotIn("region_filter", params_dict["target"])
