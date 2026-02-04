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

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pandas as pd

from evo.common.interfaces import IFeedback
from evo.common.utils import NoFeedback
from evo.objects import SchemaVersion
from evo.objects.utils.table_formats import (
    FLOAT_ARRAY_3,
    INDEX_ARRAY_1,
    INDEX_ARRAY_2,
    INDEX_ARRAY_3,
    KnownTableFormat,
)

from ._data import DataTable, DataTableAndAttributes
from ._model import DataAdapter, DataLocation, SchemaLocation
from .exceptions import ObjectValidationError
from .spatial import BaseSpatialObject, BaseSpatialObjectData
from .types import BoundingBox

__all__ = [
    "Edges",
    "Indices",
    "Parts",
    "TriangleMesh",
    "TriangleMeshData",
    "TrianglePartsData",
    "Vertices",
]

_X = "x"
_Y = "y"
_Z = "z"
_VERTEX_COLUMNS = [_X, _Y, _Z]

_N0 = "n0"
_N1 = "n1"
_N2 = "n2"
_INDEX_COLUMNS = [_N0, _N1, _N2]

# Edge indices columns (start, end)
_EDGE_START = "start"
_EDGE_END = "end"
_EDGE_INDEX_COLUMNS = [_EDGE_START, _EDGE_END]

# Chunk columns (offset, count)
_CHUNK_OFFSET = "offset"
_CHUNK_COUNT = "count"
_CHUNK_COLUMNS = [_CHUNK_OFFSET, _CHUNK_COUNT]

# Triangle indices column for parts
_TRIANGLE_INDEX = "index"
_TRIANGLE_INDEX_COLUMNS = [_TRIANGLE_INDEX]


def _bounding_box_from_dataframe(df: pd.DataFrame) -> BoundingBox:
    return BoundingBox.from_points(
        df[_X].values,
        df[_Y].values,
        df[_Z].values,
    )


# --- Generic Parts ---


class ChunksTable(DataTable):
    """DataTable for chunk definitions (offset, count columns)."""

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_2
    data_columns: ClassVar[list[str]] = _CHUNK_COLUMNS


class Parts(DataTableAndAttributes):
    """A dataset representing chunk definitions with attributes.

    Chunks define segments of indices, each with an offset and count.
    """

    _table: Annotated[ChunksTable, SchemaLocation("chunks")]

    @property
    def num_parts(self) -> int:
        """The number of parts (chunks)."""
        return self.length


# --- Triangle parts ---


@dataclass(kw_only=True, frozen=True)
class TrianglePartsData:
    """Data class for creating parts (triangle chunks) on a TriangleMesh.

    :param parts: A DataFrame containing chunk definitions. Must have 'offset', 'count' columns.
        Any additional columns will be treated as chunk attributes.
    :param triangle_indices: Optional DataFrame containing triangle indices. Must have 'index' column
        if provided. Used when chunks don't reference contiguous triangles.
    """

    parts: pd.DataFrame
    triangle_indices: pd.DataFrame | None = None

    def __post_init__(self):
        missing_chunk_cols = set(_CHUNK_COLUMNS) - set(self.parts.columns)
        if missing_chunk_cols:
            raise ObjectValidationError(
                f"parts DataFrame must have 'offset', 'count' columns. Missing: {missing_chunk_cols}"
            )
        if self.triangle_indices is not None:
            missing_ti_cols = set(_TRIANGLE_INDEX_COLUMNS) - set(self.triangle_indices.columns)
            if missing_ti_cols:
                raise ObjectValidationError(
                    f"triangle_indices DataFrame must have 'index' column. Missing: {missing_ti_cols}"
                )


class TriangleIndicesTable(DataTable):
    """DataTable for triangle indices for parts (index column)."""

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_1
    data_columns: ClassVar[list[str]] = _TRIANGLE_INDEX_COLUMNS


class TriangleParts(Parts):
    """A structure defining triangle chunks the mesh is composed of.

    Parts allow sharing common sections of one volume or surface with another.
    Parts are made up from chunks of triangle indices.
    """

    _table_data_field: ClassVar[str] = "parts"
    triangle_indices: Annotated[
        TriangleIndicesTable | None, SchemaLocation("triangle_indices"), DataLocation("triangle_indices")
    ]


# --- Edges ---


@dataclass(kw_only=True, frozen=True)
class _EdgesData:
    edges: pd.DataFrame
    edge_parts: pd.DataFrame | None = None


class EdgeIndicesTable(DataTable):
    """DataTable for edge indices (start, end columns)."""

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_2
    data_columns: ClassVar[list[str]] = _EDGE_INDEX_COLUMNS


class Edges(DataTableAndAttributes):
    """A structure defining edges and edge chunks of the mesh.

    Edges are defined by tuples of indices into the vertex list.
    Optionally, edge parts can be defined.
    """

    _table_data_field: ClassVar[str | None] = "edges"
    _table: Annotated[EdgeIndicesTable, SchemaLocation("indices")]
    parts: Annotated[Parts | None, SchemaLocation("parts"), DataLocation("edge_parts")]

    @property
    def num_edges(self) -> int:
        """The number of edges in this mesh."""
        return self._table.length


# --- Triangle Data ---


@dataclass(kw_only=True, frozen=True)
class TriangleMeshData(BaseSpatialObjectData):
    """Data class for creating a new TriangleMesh object.

    :param name: The name of the object.
    :param vertices: A DataFrame containing the vertex data. Must have 'x', 'y', 'z' columns for coordinates.
        Any additional columns will be treated as vertex attributes.
    :param triangles: A DataFrame containing the triangle indices. Must have 'n0', 'n1', 'n2' columns
        as 0-based indices into the vertices. Any additional columns will be treated as triangle attributes.
    :param edges: Optional, either an EdgesData object or a DataFrame containing edge definitions.
    :param edge_parts: Optional DataFrame defining edge chunks the mesh is composed of.
    :param coordinate_reference_system: Optional EPSG code or WKT string for the coordinate reference system.
    :param description: Optional description of the object.
    :param tags: Optional dictionary of tags for the object.
    :param extensions: Optional dictionary of extensions for the object.
    """

    vertices: pd.DataFrame
    triangles: pd.DataFrame
    edges: pd.DataFrame | None = None
    edge_parts: pd.DataFrame | None = None
    triangle_parts: pd.DataFrame | TrianglePartsData | None = None

    def __post_init__(self):
        missing_vertex_cols = set(_VERTEX_COLUMNS) - set(self.vertices.columns)
        if missing_vertex_cols:
            raise ObjectValidationError(
                f"vertices DataFrame must have 'x', 'y', 'z' columns. Missing: {missing_vertex_cols}"
            )

        missing_index_cols = set(_INDEX_COLUMNS) - set(self.triangles.columns)
        if missing_index_cols:
            raise ObjectValidationError(
                f"triangles DataFrame must have 'n0', 'n1', 'n2' columns. Missing: {missing_index_cols}"
            )

        # Validate that triangle indices are within valid range
        max_index = self.triangles[_INDEX_COLUMNS].max().max()
        num_vertices = len(self.vertices)
        if max_index >= num_vertices:
            raise ObjectValidationError(
                f"Triangle indices reference vertex index {max_index}, but only {num_vertices} vertices exist."
            )

        if self.edge_parts is not None and self.edges is None:
            raise ObjectValidationError(
                "edge_parts provided without edges. Edges must be provided if edge_parts are specified."
            )

    def compute_bounding_box(self) -> BoundingBox:
        return _bounding_box_from_dataframe(self.vertices)


class VertexCoordinateTable(DataTable):
    """DataTable subclass for vertex coordinates with x, y, z columns."""

    table_format: ClassVar[KnownTableFormat] = FLOAT_ARRAY_3
    data_columns: ClassVar[list[str]] = _VERTEX_COLUMNS

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback):
        """Update the vertex coordinate values and recalculate the bounding box.

        :param df: DataFrame containing x, y, z coordinate columns.
        :param fb: Optional feedback object to report upload progress.
        """
        await super().set_dataframe(df, fb)

        # Update the bounding box in the parent object context
        self._context.root_model.bounding_box = _bounding_box_from_dataframe(df)


class TriangleIndexTable(DataTable):
    """DataTable subclass for triangle indices with n0, n1, n2 columns."""

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_3
    data_columns: ClassVar[list[str]] = _INDEX_COLUMNS


class Vertices(DataTableAndAttributes):
    """A dataset representing the vertices of a TriangleMesh.

    Contains the coordinates of each vertex and optional attributes.
    """

    _table: VertexCoordinateTable


class Indices(DataTableAndAttributes):
    """A dataset representing the triangle indices of a TriangleMesh.

    Contains indices into the vertex list defining triangles and optional attributes.
    """

    _table: TriangleIndexTable


def _extract_edges_data(data: TriangleMeshData) -> _EdgesData | None:
    """Extract edges and edge_parts DataFrames from various input types."""
    if data.edges is None:
        return None
    return _EdgesData(edges=data.edges, edge_parts=data.edge_parts)


def _extract_parts_data(data: TriangleMeshData) -> TrianglePartsData | None:
    """Extract triangle parts data from various input types."""
    if data.triangle_parts is None:
        return None
    if isinstance(data.triangle_parts, pd.DataFrame):
        return TrianglePartsData(parts=data.triangle_parts)
    return data.triangle_parts


class TriangleMesh(BaseSpatialObject):
    """A GeoscienceObject representing a mesh composed of triangles.

    The triangles are defined by triplets of indices into a vertex list.
    The object contains vertices, indices, and optional attributes
    for both vertices and triangles.

    Optionally, parts and edges can be defined:
    - parts: Define triangle chunks the mesh is composed of, allowing sharing common
      sections between volumes or surfaces.
    - edges: Define edges as tuples of vertex indices, optionally with edge parts.
    """

    _data_class = TriangleMeshData

    sub_classification = "triangle-mesh"
    creation_schema_version = SchemaVersion(major=2, minor=2, patch=0)

    vertices: Annotated[Vertices, SchemaLocation("triangles.vertices"), DataLocation("vertices")]
    indices: Annotated[Indices, SchemaLocation("triangles.indices"), DataLocation("triangles")]
    parts: Annotated[TriangleParts | None, SchemaLocation("parts"), DataAdapter(_extract_parts_data)]
    edges: Annotated[Edges | None, SchemaLocation("edges"), DataAdapter(_extract_edges_data)]

    @property
    def num_vertices(self) -> int:
        """The number of vertices in this mesh."""
        return self.vertices.length

    @property
    def num_triangles(self) -> int:
        """The number of triangles in this mesh."""
        return self.indices.length

    @property
    def num_edges(self) -> int | None:
        """The number of edges in this mesh, or None if edges are not defined."""
        if self.edges is None:
            return None
        return self.edges.num_edges

    @property
    def num_parts(self) -> int | None:
        """The number of parts in this mesh, or None if parts are not defined."""
        if self.parts is None:
            return None
        return self.parts.length

    async def get_vertices_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the vertex coordinates and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with x, y, z coordinates and additional columns for attributes.
        """
        return await self.vertices.get_dataframe(fb=fb)

    async def set_vertices_dataframe(self, data: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the vertices data on this mesh.

        :param data: DataFrame containing the vertex coordinates and attributes.
        :param fb: Optional feedback object to report upload progress.
        """
        await self.vertices.set_dataframe(data, fb=fb)

    async def get_indices_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the triangle indices and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with n0, n1, n2 indices and additional columns for attributes.
        """
        return await self.indices.get_dataframe(fb=fb)

    async def set_indices_dataframe(self, data: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the triangle indices data on this mesh.

        :param data: DataFrame containing the triangle indices and attributes.
        :param fb: Optional feedback object to report upload progress.
        """
        await self.indices.set_dataframe(data, fb=fb)

    async def get_edges_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame | None:
        """Load a DataFrame containing the edge definitions.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with start, end indices and additional columns for attributes, or None if edges are not defined.
        """
        if self.edges is None:
            return None
        return await self.edges.get_dataframe(fb=fb)

    async def set_edges_dataframe(self, data: pd.DataFrame, parts: pd.DataFrame | None = None) -> None:
        """Set the edges data on this mesh.

        :param data: EdgesData containing the edge definitions.
        """
        await self._build_sub_model("edges", _EdgesData(edges=data, edge_parts=parts))

    async def get_parts_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame | None:
        """Load a DataFrame containing the parts definitions.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with offset, count columns and additional columns for attributes, or None if parts are not defined.
        """
        if self.parts is None:
            return None
        return await self.parts.get_dataframe(fb=fb)

    async def set_parts_dataframe(
        self, data: pd.DataFrame, triangle_indices: pd.DataFrame | None = None, fb: IFeedback = NoFeedback
    ) -> None:
        """Set the parts data on this mesh.

        :param data: PartsData containing the part definitions.
        :param fb: Optional feedback object to report upload progress.
        """
        await self._build_sub_model("parts", TrianglePartsData(parts=data, triangle_indices=triangle_indices))
