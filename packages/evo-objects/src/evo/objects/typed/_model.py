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

import copy
import operator
import types
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import (
    Annotated,
    Any,
    Callable,
    ClassVar,
    Generic,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pydantic import TypeAdapter

from evo import jmespath
from evo.common import IContext
from evo.objects import DownloadedObject

from ._utils import (
    assign_jmespath_value,
    delete_jmespath_value,
)

_T = TypeVar("_T")


@dataclass
class ModelContext:
    """Context for the schema models."""

    obj: DownloadedObject
    """The DownloadedObject associated with this context."""

    root_model: SchemaModel
    """The root SchemaModel for this context."""

    data_modified: set[str] = field(default_factory=set)
    """Flags indicating which data fields have been modified.
    """

    def mark_modified(self, data_ref: str) -> None:
        """Mark that a specific data field has been modified and should not be loaded."""
        self.data_modified.add(data_ref)

    def is_data_modified(self, data_ref: str) -> bool:
        """Check if a specific data field has been marked as modified."""
        return data_ref in self.data_modified


@dataclass
class SchemaLocation:
    """Metadata for annotating a field's location within a Geoscience Object schema."""

    jmespath_expr: str
    """The JMESPath expression to locate this field in the document."""


@dataclass(frozen=True)
class DataLocation:
    """Metadata for annotating a field's location within a data classes used to generate Geoscience Object data."""

    field_path: str
    """The path to the field within the data class."""


@dataclass(frozen=True)
class DataAdapter:
    """Metadata for annotating a field that requires custom data access logic."""

    getter: Callable[[Any], Any]
    """A function to get the data for this field."""


@dataclass(frozen=True)
class SubModelMetadata:
    """Metadata for sub-model fields within a SchemaModel."""

    model_type: type[SchemaModel]
    """The type of the sub-model."""

    jmespath_expr: str | None
    """The JMESPath expression locating the sub-model in the document."""

    data_adapter: Callable[[Any], Any] | None = None
    """Function to extract the sub-model data from the parent data object."""

    is_optional: bool = False
    """Whether this sub-model is optional (can be None)."""


def _is_optional_type(annotation: Any) -> tuple[Any, bool]:
    """Check if an annotation is an Optional type (Union with None).

    Returns a tuple of (base_type, is_optional).
    If the annotation is Optional[X] or X | None, returns (X, True).
    Otherwise returns (annotation, False).
    """
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        args = get_args(annotation)
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1 and len(args) == 2:
            return non_none_args[0], True
    return annotation, False


class SchemaProperty(Generic[_T]):
    """Descriptor for data within a Geoscience Object schema.

    This can be used on either typed objects classes or dataset classes.
    """

    def __init__(
        self,
        jmespath_expr: str,
        type_adapter: TypeAdapter[_T],
    ) -> None:
        self._jmespath_expr = jmespath_expr
        self._type_adapter = type_adapter

    @property
    def jmespath_expr(self) -> str:
        return self._jmespath_expr

    def dump_value(self, value: _T) -> Any:
        """Dump a value using the TypeAdapter."""
        return self._type_adapter.dump_python(value)

    @overload
    def __get__(self, instance: None, owner: type[SchemaModel]) -> SchemaProperty[_T]: ...

    @overload
    def __get__(self, instance: SchemaModel, owner: type[SchemaModel]) -> _T: ...

    def __get__(self, instance: SchemaModel | None, owner: type[SchemaModel]) -> Any:
        if instance is None:
            return self

        value = instance.search(self._jmespath_expr)
        if isinstance(value, (jmespath.JMESPathArrayProxy, jmespath.JMESPathObjectProxy)):
            value = value.raw
        # Use TypeAdapter to validate and apply defaults from Field annotations
        return self._type_adapter.validate_python(value)

    def __set__(self, instance: SchemaModel, value: Any) -> None:
        _set_property_value(self, instance._document, value)


def _set_property_value(schema_property: SchemaProperty, document: dict[str, Any], value: Any) -> None:
    """Set the value of a SchemaProperty by name.

    :param property_name: The name of the property to set.
    :param value: The value to set the property to.
    """
    dumped_value = schema_property.dump_value(value)

    if dumped_value is None:
        # Remove the property from the document if the value is None
        delete_jmespath_value(document, schema_property.jmespath_expr)
    else:
        # Update the document with the new value
        assign_jmespath_value(document, schema_property.jmespath_expr, dumped_value)


class SchemaBuilder:
    """Helper class to build a schema document by applying SchemaProperty values."""

    def __init__(self, schema_model_cls: type[SchemaModel], context: ModelContext) -> None:
        self.document: dict[str, Any] = {}
        self._properties = schema_model_cls._schema_properties
        self._sub_models = schema_model_cls._sub_models
        self._context = context

    def set_property(self, name: str, value: Any) -> None:
        schema_property = self._properties[name]
        _set_property_value(schema_property, self.document, value)

    async def set_sub_model_value(self, name: str, data: Any) -> None:
        metadata = self._sub_models[name].metadata
        sub_document = await metadata.model_type._data_to_schema(data, context=self._context)
        if metadata.jmespath_expr:
            assign_jmespath_value(self.document, metadata.jmespath_expr, sub_document)
        else:
            self.document.update(sub_document)


def _get_annotation_metadata(annotation: Any) -> tuple[Any, dict[type, Any]]:
    """Extract the base type and metadata from an Annotated type.

    :param annotation: The type annotation to process.
    :return: A tuple of (base_type, metadata_dict) where metadata_dict maps
             metadata class types to their instances.
    """
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        if len(args) >= 2:
            metadata = {type(m): m for m in args[1:]}
            return args[0], metadata
    return annotation, {}


class SubModelProperty(Generic[_T]):
    """Descriptor for sub-model fields within a SchemaModel.

    This handles both getting and deleting sub-models. Setting is handled
    via async methods since it may require data uploads.
    """

    def __init__(self, metadata: SubModelMetadata) -> None:
        self._metadata = metadata
        self._attr_name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr_name = f"_submodel_{name}"

    @overload
    def __get__(self, instance: None, owner: type[SchemaModel]) -> SubModelProperty[_T]: ...

    @overload
    def __get__(self, instance: SchemaModel, owner: type[SchemaModel]) -> _T | None: ...

    def __get__(self, instance: SchemaModel | None, owner: type[SchemaModel]) -> Any:
        if instance is None:
            return self
        return getattr(instance, self._attr_name, None)

    def __set__(self, instance: SchemaModel, value: _T | None) -> None:
        object.__setattr__(instance, self._attr_name, value)

    def __delete__(self, instance: SchemaModel) -> None:
        if not self._metadata.is_optional:
            raise AttributeError("Cannot delete required sub-model")
        if self._metadata.jmespath_expr:
            delete_jmespath_value(instance._document, self._metadata.jmespath_expr)
        object.__setattr__(instance, self._attr_name, None)

    @property
    def metadata(self) -> SubModelMetadata:
        return self._metadata


class SchemaModel:
    """Base class for models backed by a Geoscience Object schema.

    The data is stored in the underlying document dictionary. Sub-models are
    automatically created for nested SchemaModel/SchemaList fields.
    """

    _schema_properties: ClassVar[dict[str, SchemaProperty[Any]]] = {}
    _sub_models: ClassVar[dict[str, SubModelProperty[Any]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Initialize with inherited values (copy to avoid mutating parent)
        schema_properties: dict[str, SchemaProperty[Any]] = {}
        sub_models: dict[str, SubModelProperty[Any]] = {}
        for base in cls.__mro__[1:]:
            if issubclass(base, SchemaModel):
                schema_properties.update(base._schema_properties)
                sub_models.update(base._sub_models)

        # Resolve string annotations using get_type_hints
        try:
            # include_extras=True preserves Annotated metadata
            hints = get_type_hints(cls, include_extras=True)
        except Exception:
            # If get_type_hints fails, use inherited values
            cls._schema_properties = schema_properties
            cls._sub_models = sub_models
            return

        # Process the resolved annotations
        for field_name, annotation in hints.items():
            if get_origin(annotation) is ClassVar:
                continue  # Skip ClassVar fields
            base_type, metadata = _get_annotation_metadata(annotation)

            schema_location = metadata.get(SchemaLocation)
            data_adapter = metadata.get(DataAdapter)
            if data_adapter is None:
                data_location = metadata.get(DataLocation)
                if data_location is not None:
                    data_adapter_function = operator.attrgetter(data_location.field_path)
                else:
                    # Identity function if no DataAdapter or DataLocation is provided
                    def data_adapter_function(data):
                        return data
            else:
                data_adapter_function = data_adapter.getter

            # Check if this is an optional type (X | None)
            inner_type, is_optional = _is_optional_type(base_type)

            # To robustly check for SchemaModel/SchemaList, we need to strip any generic or Annotated wrappers
            bare_base_type = get_origin(inner_type) or inner_type
            if isinstance(bare_base_type, type) and issubclass(bare_base_type, (SchemaModel, SchemaList)):
                sub_model_metadata = SubModelMetadata(
                    model_type=inner_type,
                    jmespath_expr=schema_location.jmespath_expr if schema_location else None,
                    data_adapter=data_adapter_function,
                    is_optional=is_optional,
                )
                prop = SubModelProperty(sub_model_metadata)
                # Manually call __set_name__ since setattr doesn't trigger it
                prop.__set_name__(cls, field_name)
                setattr(cls, field_name, prop)
                sub_models[field_name] = prop
            else:
                if schema_location is None:
                    raise ValueError(
                        f"Non-submodel field '{field_name}' in '{cls.__name__}' is missing a SchemaLocation annotation."
                    )
                # Create a TypeAdapter for the full annotation (preserves Field defaults)
                type_adapter = TypeAdapter(annotation)

                # Create SchemaProperty descriptor and set it on the class
                prop = SchemaProperty(
                    jmespath_expr=schema_location.jmespath_expr,
                    type_adapter=type_adapter,
                )
                setattr(cls, field_name, prop)
                schema_properties[field_name] = prop

        cls._schema_properties = schema_properties
        cls._sub_models = sub_models

    def __init__(self, context: ModelContext | DownloadedObject, document: dict[str, Any]) -> None:
        """Initialize the SchemaModel.

        :param context: Either a ModelContext or a DownloadedObject this model is associated with.
        :param document: The document dictionary representing the Geoscience Object.
        """
        if isinstance(context, DownloadedObject):
            self._context = ModelContext(obj=context, root_model=self)
        else:
            self._context = context
        self._document = document

        self._rebuild_models()

    @property
    def _obj(self) -> DownloadedObject:
        """Get the DownloadedObject for this model.

        :raises DataLoaderError: If this model was created without a DownloadedObject.
        """
        return self._context.obj

    def _rebuild_models(self) -> None:
        """Rebuild any sub-models to reflect changes in the underlying document."""
        for sub_model_name, sub_model_prop in self._sub_models.items():
            metadata = sub_model_prop.metadata
            if metadata.jmespath_expr:
                sub_document = jmespath.search(metadata.jmespath_expr, self._document)
                if sub_document is None:
                    # For optional sub-models, set the attribute to None if not present
                    if metadata.is_optional:
                        setattr(self, sub_model_name, None)
                        continue
                    # Initialize an empty list/dict for required sub-models if not present
                    if issubclass(metadata.model_type, SchemaList):
                        sub_document = []
                    else:
                        sub_document = {}
                    assign_jmespath_value(self._document, metadata.jmespath_expr, sub_document)
                else:
                    # Unwrap jmespath proxy to get raw data for mutation
                    sub_document = sub_document.raw
            else:
                sub_document = self._document
            setattr(self, sub_model_name, metadata.model_type(self._context, sub_document))

    def validate(self) -> None:
        """Validate the model is valid."""
        for sub_model_name in self._sub_models:
            sub_model = getattr(self, sub_model_name, None)
            if sub_model is not None:
                sub_model.validate()

    @classmethod
    async def _build_schema(cls, builder: SchemaBuilder, data: Any, exclude: set[str]) -> None:
        """Build a schema document using the provided SchemaBuilder."""
        for key in cls._schema_properties.keys():
            if key in exclude:
                continue
            value = getattr(data, key, None)
            builder.set_property(key, value)
        for name, sub_model_prop in cls._sub_models.items():
            if name in exclude:
                continue
            metadata = sub_model_prop.metadata
            sub_data = metadata.data_adapter(data)
            # Skip optional sub-models when their data is None
            if sub_data is None and metadata.is_optional:
                continue
            await builder.set_sub_model_value(name, sub_data)

    @classmethod
    async def _data_to_schema(cls, data: Any, context: IContext) -> Any:
        """Convert data to a dictionary by applying schema properties.

        This base implementation iterates over all schema properties defined on the class
        and applies their values from the data object to the result dictionary.

        :param data: The data object containing values to convert.
        :param context: The context used for any data upload operations.
        :return: The dictionary representation of the data.
        """
        builder = SchemaBuilder(cls, context)
        await cls._build_schema(builder, data, exclude=set())
        return builder.document

    async def _build_sub_model(self, name: str, data: Any) -> None:
        """Rebuild all sub-models from the current document."""
        metadata = self._sub_models[name].metadata
        sub_document = await metadata.model_type._data_to_schema(data, context=self._context)
        if metadata.jmespath_expr:
            assign_jmespath_value(self._document, metadata.jmespath_expr, sub_document)
            document = sub_document
        else:
            self._document.update(sub_document)
            document = self._document
        setattr(self, name, metadata.model_type(self._context, document))

    def search(self, expression: str) -> Any:
        """Search the model using a JMESPath expression.

        :param expression: The JMESPath expression to use for the search.

        :return: The result of the search.
        """
        return jmespath.search(expression, self._document)

    def as_dict(self) -> dict[str, Any]:
        """Get the model as a dictionary.

        :return: The model as a dictionary.
        """
        return copy.deepcopy(self._document)


_M = TypeVar("_M", bound=SchemaModel)


class SchemaList(Sequence[_M]):
    """A list of SchemaModel instances backed by a list in the document."""

    _item_type: type[_M]

    def __init__(self, context: ModelContext | DownloadedObject, document: list[Any]) -> None:
        if isinstance(context, DownloadedObject):
            self._context = ModelContext(obj=context, root_model=self)
        else:
            self._context = context
        self._document = document

    @property
    def _obj(self) -> DownloadedObject:
        """Get the DownloadedObject for this model list."""
        return self._context.obj

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Extract the item type from the generic parameter
        for base in cls.__orig_bases__:
            if get_origin(base) is SchemaList:
                args = get_args(base)
                if args:
                    cls._item_type = args[0]
                    break

    def __getitem__(self, index: int) -> _M:
        return self._item_type(self._context, self._document[index])

    def __iter__(self):
        for item in self._document:
            yield self._item_type(self._context, item)

    def __len__(self) -> int:
        return len(self._document)

    def clear(self) -> None:
        """Clear all items from the list."""
        self._document.clear()

    def _append(self, value: _M) -> None:
        """Append an item to the list."""
        self._document.append(value.as_dict())

    def validate(self) -> None:
        """Validate all items in the list."""
        for item in self:
            item.validate()

    @classmethod
    async def _data_to_schema(cls, data: Any, context: IContext) -> list[Any]:
        if data is None:
            return []
        if not isinstance(data, Sequence):
            raise TypeError(f"Expected a sequence for SchemaList data, got {type(data).__name__}")
        return [await cls._item_type._data_to_schema(item, context) for item in data]
