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
from typing import Annotated, Any, ClassVar

import pandas as pd

from evo.common.interfaces import IContext, IFeedback
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
from ._model import DataLocation, SchemaBuilder, SchemaLocation, SchemaModel
from .attributes import Attributes
from .exceptions import ObjectValidationError
from .spatial import BaseSpatialObject, BaseSpatialObjectData
from .types import BoundingBox

__all__ = [
    "EdgeIndices",
    "EdgeParts",
    "EdgePartsData",
    "Edges",
    "EdgesData",
    "Indices",
    "Parts",
    "PartsData",
    "TriangleMesh",
    "TriangleMeshData",
    "Triangles",
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


@dataclass(kw_only=True, frozen=True)
class TriangleMeshData(BaseSpatialObjectData):
    """Data class for creating a new TriangleMesh object.

    :param name: The name of the object.
    :param vertices: A DataFrame containing the vertex data. Must have 'x', 'y', 'z' columns for coordinates.
        Any additional columns will be treated as vertex attributes.
    :param triangles: A DataFrame containing the triangle indices. Must have 'n0', 'n1', 'n2' columns
        as 0-based indices into the vertices. Any additional columns will be treated as triangle attributes.
    :param edges: Optional EdgesData defining edges as vertex index pairs. Can include edge parts.
    :param parts: Optional PartsData defining triangle chunks the mesh is composed of.
    :param coordinate_reference_system: Optional EPSG code or WKT string for the coordinate reference system.
    :param description: Optional description of the object.
    :param tags: Optional dictionary of tags for the object.
    :param extensions: Optional dictionary of extensions for the object.
    """

    vertices: pd.DataFrame
    triangles: pd.DataFrame
    edges: EdgesData | None = None
    parts: PartsData | None = None

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

    _table: Annotated[VertexCoordinateTable, SchemaLocation("")]


class Indices(DataTableAndAttributes):
    """A dataset representing the triangle indices of a TriangleMesh.

    Contains indices into the vertex list defining triangles and optional attributes.
    """

    _table: Annotated[TriangleIndexTable, SchemaLocation("")]


# --- Parts support for triangle mesh ---


class Chunks(DataTable):
    """DataTable for chunk definitions (offset, count columns).

    Chunks define segments of indices, each with an offset and count.
    """

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_2
    data_columns: ClassVar[list[str]] = _CHUNK_COLUMNS


class TriangleIndices(DataTable):
    """DataTable for triangle indices for parts (index column).

    Optional index array into the triangle indices set. Used to define chunks
    if the mesh triangle indices do not contain contiguous chunks.
    """

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_1
    data_columns: ClassVar[list[str]] = _TRIANGLE_INDEX_COLUMNS


@dataclass(kw_only=True, frozen=True)
class PartsData:
    """Data class for creating parts (triangle chunks) on a TriangleMesh.

    :param chunks: A DataFrame containing chunk definitions. Must have 'offset', 'count' columns.
        Any additional columns will be treated as chunk attributes.
    :param triangle_indices: Optional DataFrame containing triangle indices. Must have 'index' column
        if provided. Used when chunks don't reference contiguous triangles.
    """

    chunks: pd.DataFrame
    triangle_indices: pd.DataFrame | None = None

    def __post_init__(self):
        missing_chunk_cols = set(_CHUNK_COLUMNS) - set(self.chunks.columns)
        if missing_chunk_cols:
            raise ObjectValidationError(
                f"chunks DataFrame must have 'offset', 'count' columns. Missing: {missing_chunk_cols}"
            )
        if self.triangle_indices is not None:
            missing_ti_cols = set(_TRIANGLE_INDEX_COLUMNS) - set(self.triangle_indices.columns)
            if missing_ti_cols:
                raise ObjectValidationError(
                    f"triangle_indices DataFrame must have 'index' column. Missing: {missing_ti_cols}"
                )


class Parts(SchemaModel):
    """A structure defining triangle chunks the mesh is composed of.

    Parts allow sharing common sections of one volume or surface with another.
    Parts are made up from chunks of triangle indices.
    """

    chunks: Annotated[Chunks, SchemaLocation("chunks"), DataLocation("chunks")]
    triangle_indices: Annotated[
        TriangleIndices | None, SchemaLocation("triangle_indices"), DataLocation("triangle_indices")
    ]
    attributes: Annotated[Attributes, SchemaLocation("attributes")]

    @property
    def num_parts(self) -> int:
        """The number of parts (chunks) defined."""
        return self.chunks.length

    async def get_chunks_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the chunk definitions and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with offset, count columns and additional columns for attributes.
        """
        chunks_df = await self.chunks.get_dataframe(fb=fb)
        if self.attributes is not None and len(self.attributes) > 0:
            attr_df = await self.attributes.get_dataframe(fb=fb)
            return pd.concat([chunks_df, attr_df], axis=1)
        return chunks_df

    async def get_triangle_indices_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame | None:
        """Load a DataFrame containing the triangle indices if present.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with index column, or None if not present.
        """
        if self.triangle_indices is None or self.triangle_indices.length == 0:
            return None
        return await self.triangle_indices.get_dataframe(fb=fb)

    async def set_chunks_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the chunk data from a DataFrame.

        :param df: DataFrame with 'offset', 'count' columns and optional attribute columns.
        :param fb: Optional feedback object to report upload progress.
        """
        chunks_df, attr_df = DataTableAndAttributes._split_dataframe(df, _CHUNK_COLUMNS)
        await self.chunks.set_dataframe(chunks_df, fb=fb)
        if attr_df is not None:
            await self.attributes.set_attributes(attr_df, fb=fb)
        else:
            await self.attributes.clear()

    async def set_triangle_indices_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the triangle indices data from a DataFrame.

        :param df: DataFrame with 'index' column.
        :param fb: Optional feedback object to report upload progress.
        """
        if self.triangle_indices is None:
            raise ObjectValidationError(
                "Cannot set triangle_indices on a Parts object that was not initialized with triangle_indices."
            )
        # Triangle indices don't have separate attributes
        ti_df = df[_TRIANGLE_INDEX_COLUMNS]
        await self.triangle_indices.set_dataframe(ti_df, fb=fb)

    @classmethod
    async def _data_to_schema(cls, data: PartsData, context: IContext) -> Any:
        """Convert parts data to schema format."""
        builder = SchemaBuilder(cls, context)
        # Split chunks dataframe into data and attributes
        chunks_df, attr_df = DataTableAndAttributes._split_dataframe(data.chunks, _CHUNK_COLUMNS)
        await builder.set_sub_model_value("chunks", chunks_df)
        if data.triangle_indices is not None:
            # Triangle indices are just data, no attributes
            ti_df = data.triangle_indices[_TRIANGLE_INDEX_COLUMNS]
            await builder.set_sub_model_value("triangle_indices", ti_df)
        await builder.set_sub_model_value("attributes", attr_df)
        return builder.document


# --- Edge support for triangle mesh ---


class EdgeIndices(DataTable):
    """DataTable for edge indices of a TriangleMesh.

    Contains tuples of vertex indices defining edges (start, end columns).
    """

    table_format: ClassVar[KnownTableFormat] = INDEX_ARRAY_2
    data_columns: ClassVar[list[str]] = _EDGE_INDEX_COLUMNS


@dataclass(kw_only=True, frozen=True)
class EdgePartsData:
    """Data class for creating edge parts (edge chunks) on a TriangleMesh.

    :param chunks: A DataFrame containing edge chunk definitions. Must have 'offset', 'count' columns.
        Any additional columns will be treated as chunk attributes.
    """

    chunks: pd.DataFrame

    def __post_init__(self):
        missing_chunk_cols = set(_CHUNK_COLUMNS) - set(self.chunks.columns)
        if missing_chunk_cols:
            raise ObjectValidationError(
                f"chunks DataFrame must have 'offset', 'count' columns. Missing: {missing_chunk_cols}"
            )


class EdgeParts(SchemaModel):
    """A structure defining edge chunks of the mesh.

    Edge parts define segments of the edges array.
    """

    chunks: Annotated[Chunks, SchemaLocation("chunks"), DataLocation("chunks")]
    attributes: Annotated[Attributes, SchemaLocation("attributes")]

    @property
    def num_parts(self) -> int:
        """The number of edge parts (chunks) defined."""
        return self.chunks.length

    async def get_chunks_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the edge chunk definitions and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with offset, count columns and additional columns for attributes.
        """
        chunks_df = await self.chunks.get_dataframe(fb=fb)
        if self.attributes is not None and len(self.attributes) > 0:
            attr_df = await self.attributes.get_dataframe(fb=fb)
            return pd.concat([chunks_df, attr_df], axis=1)
        return chunks_df

    async def set_chunks_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the edge chunk data from a DataFrame.

        :param df: DataFrame with 'offset', 'count' columns and optional attribute columns.
        :param fb: Optional feedback object to report upload progress.
        """
        chunks_df, attr_df = DataTableAndAttributes._split_dataframe(df, _CHUNK_COLUMNS)
        await self.chunks.set_dataframe(chunks_df, fb=fb)
        if attr_df is not None:
            await self.attributes.set_attributes(attr_df, fb=fb)
        else:
            await self.attributes.clear()

    @classmethod
    async def _data_to_schema(cls, data: EdgePartsData, context: IContext) -> Any:
        """Convert edge parts data to schema format."""
        builder = SchemaBuilder(cls, context)
        # Split chunks dataframe into data and attributes
        chunks_df, attr_df = DataTableAndAttributes._split_dataframe(data.chunks, _CHUNK_COLUMNS)
        await builder.set_sub_model_value("chunks", chunks_df)
        await builder.set_sub_model_value("attributes", attr_df)
        return builder.document


@dataclass(kw_only=True, frozen=True)
class EdgesData:
    """Data class for creating edges on a TriangleMesh.

    :param indices: A DataFrame containing edge definitions. Must have 'start', 'end' columns
        as 0-based indices into the vertices. Any additional columns will be treated as edge attributes.
    :param parts: Optional EdgePartsData for defining edge chunks.
    """

    indices: pd.DataFrame
    parts: EdgePartsData | None = None

    def __post_init__(self):
        missing_index_cols = set(_EDGE_INDEX_COLUMNS) - set(self.indices.columns)
        if missing_index_cols:
            raise ObjectValidationError(
                f"indices DataFrame must have 'start', 'end' columns. Missing: {missing_index_cols}"
            )


class Edges(SchemaModel):
    """A structure defining edges and edge chunks of the mesh.

    Edges are defined by tuples of indices into the vertex list.
    Optionally, edge parts can be defined.
    """

    indices: Annotated[EdgeIndices, SchemaLocation("indices"), DataLocation("indices")]
    parts: Annotated[EdgeParts | None, SchemaLocation("parts"), DataLocation("parts")]
    attributes: Annotated[Attributes, SchemaLocation("attributes")]

    @property
    def num_edges(self) -> int:
        """The number of edges in this mesh."""
        return self.indices.length

    async def get_indices_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the edge indices and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with start, end columns and additional columns for attributes.
        """
        indices_df = await self.indices.get_dataframe(fb=fb)
        if self.attributes is not None and len(self.attributes) > 0:
            attr_df = await self.attributes.get_dataframe(fb=fb)
            return pd.concat([indices_df, attr_df], axis=1)
        return indices_df

    async def set_indices_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the edge indices data from a DataFrame.

        :param df: DataFrame with 'start', 'end' columns and optional attribute columns.
        :param fb: Optional feedback object to report upload progress.
        """
        indices_df, attr_df = DataTableAndAttributes._split_dataframe(df, _EDGE_INDEX_COLUMNS)
        await self.indices.set_dataframe(indices_df, fb=fb)
        if attr_df is not None:
            await self.attributes.set_attributes(attr_df, fb=fb)
        else:
            await self.attributes.clear()

    @classmethod
    async def _data_to_schema(cls, data: EdgesData, context: IContext) -> Any:
        """Convert edges data to schema format."""
        builder = SchemaBuilder(cls, context)
        # Split indices dataframe into data and attributes
        indices_df, attr_df = DataTableAndAttributes._split_dataframe(data.indices, _EDGE_INDEX_COLUMNS)
        await builder.set_sub_model_value("indices", indices_df)
        if data.parts is not None:
            await builder.set_sub_model_value("parts", data.parts)
        await builder.set_sub_model_value("attributes", attr_df)
        return builder.document


@dataclass(kw_only=True, frozen=True)
class _TrianglesData:
    """Internal data class for the triangles component."""

    vertices: pd.DataFrame
    triangles: pd.DataFrame


class Triangles(SchemaModel):
    """A dataset representing the triangles of a TriangleMesh.

    This is the top-level container for the triangles component of the mesh,
    containing both vertices and triangle indices.
    """

    vertices: Annotated[Vertices, SchemaLocation("vertices"), DataLocation("vertices")]
    indices: Annotated[Indices, SchemaLocation("indices"), DataLocation("triangles")]

    @property
    def num_vertices(self) -> int:
        """The number of vertices in this mesh."""
        return self.vertices.length

    @property
    def num_triangles(self) -> int:
        """The number of triangles in this mesh."""
        return self.indices.length

    async def get_vertices_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the vertex coordinates and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with x, y, z coordinates and additional columns for attributes.
        """
        return await self.vertices.get_dataframe(fb=fb)

    async def get_indices_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the triangle indices and attributes.

        :param fb: Optional feedback object to report download progress.
        :return: DataFrame with n0, n1, n2 indices and additional columns for attributes.
        """
        return await self.indices.get_dataframe(fb=fb)

    @classmethod
    async def _data_to_schema(cls, data: Any, context: IContext) -> Any:
        """Convert triangles data to schema format."""
        builder = SchemaBuilder(cls, context)
        await builder.set_sub_model_value("vertices", data.vertices)
        await builder.set_sub_model_value("indices", data.triangles)
        return builder.document


class TriangleMesh(BaseSpatialObject):
    """A GeoscienceObject representing a mesh composed of triangles.

    The triangles are defined by triplets of indices into a vertex list.
    The object contains a triangles dataset with vertices, indices, and optional attributes
    for both vertices and triangles.

    Optionally, parts and edges can be defined:
    - parts: Define triangle chunks the mesh is composed of, allowing sharing common
      sections between volumes or surfaces.
    - edges: Define edges as tuples of vertex indices, optionally with edge parts.
    """

    _data_class = TriangleMeshData

    sub_classification = "triangle-mesh"
    creation_schema_version = SchemaVersion(major=2, minor=2, patch=0)

    triangles: Annotated[Triangles, SchemaLocation("triangles")]
    parts: Annotated[Parts | None, SchemaLocation("parts")]
    edges: Annotated[Edges | None, SchemaLocation("edges")]

    @classmethod
    async def _data_to_schema(cls, data: TriangleMeshData, context: IContext) -> dict[str, Any]:
        """Create an object dictionary suitable for creating a new Geoscience Object."""
        object_dict = await super()._data_to_schema(data, context)
        # Create the triangles data structure
        triangles_data = _TrianglesData(vertices=data.vertices, triangles=data.triangles)
        object_dict["triangles"] = await Triangles._data_to_schema(triangles_data, context)
        # Add optional edges if provided
        if data.edges is not None:
            object_dict["edges"] = await Edges._data_to_schema(data.edges, context)
        # Add optional parts if provided
        if data.parts is not None:
            object_dict["parts"] = await Parts._data_to_schema(data.parts, context)
        return object_dict

    @property
    def num_vertices(self) -> int:
        """The number of vertices in this mesh."""
        return self.triangles.num_vertices

    @property
    def num_triangles(self) -> int:
        """The number of triangles in this mesh."""
        return self.triangles.num_triangles

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
        return self.parts.num_parts

    async def set_edges(self, data: EdgesData, fb: IFeedback = NoFeedback) -> None:
        """Set the edges data on this mesh.

        :param data: EdgesData containing the edge definitions.
        :param fb: Optional feedback object to report upload progress.
        """
        edges_schema = await Edges._data_to_schema(data, self._context)
        self._document["edges"] = edges_schema
        self._rebuild_models()

    async def set_parts(self, data: PartsData, fb: IFeedback = NoFeedback) -> None:
        """Set the parts data on this mesh.

        :param data: PartsData containing the part definitions.
        :param fb: Optional feedback object to report upload progress.
        """
        parts_schema = await Parts._data_to_schema(data, self._context)
        self._document["parts"] = parts_schema
        self._rebuild_models()

    def clear_edges(self) -> None:
        """Remove edges from this mesh."""
        if "edges" in self._document:
            del self._document["edges"]
        self._rebuild_models()

    def clear_parts(self) -> None:
        """Remove parts from this mesh."""
        if "parts" in self._document:
            del self._document["parts"]
        self._rebuild_models()
