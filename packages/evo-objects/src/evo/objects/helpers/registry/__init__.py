from __future__ import annotations

import bisect
from typing import ClassVar

from evo.objects import ObjectSchema, SchemaVersion

from ..loaders import DatasetLoader

__all__ = ["DatasetLoaderRegistry"]


class DatasetLoaderRegistry:
    """Registry for DatasetLoaders based on a geoscience object schema classification andversion."""

    __REGISTRIES: ClassVar[dict[str, DatasetLoaderRegistry]] = {}
    """A class-level dictionary mapping object type (classification) to its corresponding DatasetLoaderRegistry instance.
    
    This allows for quick retrieval of the appropriate registry for a given object type, and is required for the
    static method implementation of a global loader registry.
    """

    def __init__(self, object_type: str) -> None:
        """
        :param object_type: The classification of the object type this registry manages loaders for.
        """
        self._object_type = object_type
        self._loaders: dict[SchemaVersion, DatasetLoader] = {}

    def register_version_loader(self, schema: ObjectSchema, loader: DatasetLoader) -> None:
        """Register an DatasetLoader for a specific ObjectSchema version.

        :param schema: The ObjectSchema defining the classification and version for which the loader is registered.
        :param loader: The DatasetLoader to register for the given schema version.

        :raises ValueError: If the schema classification does not match the registry's object type,
            or if a loader is already registered for the given schema version.
        """
        if schema.classification != self._object_type:
            raise ValueError(
                f"Cannot register loader for schema classification '{schema.classification}'"
                f" in registry for '{self._object_type}'"
            )
        if schema.version in self._loaders:
            raise ValueError(f"A loader is already registered for schema version '{schema.version}'")
        self._loaders[schema.version] = loader

    def resolve_version_loader(self, schema: ObjectSchema) -> DatasetLoader:
        """Resolve the most appropriate DatasetLoader for the given ObjectSchema.

        DatasetLoaders are resolved based on the following rules:

        1. The schema classification must match the registry's object type.
        2. The loader with the closest version greater than or equal to the target version is preferred.
        3. If no greater version exists, the loader with the closest lower version is used.
        4. Major version mismatches are not allowed; only loaders with the same major version as the target
            schema are considered.

        :param schema: The ObjectSchema to resolve the loader for.

        :return: The most appropriate DatasetLoader for the given schema.

        :raises ValueError: If the schema classification does not match the registry's object type,
            or if no suitable loader is found for the given schema version.
        """
        if schema.classification != self._object_type:
            raise ValueError(
                f"Cannot resolve loader for schema classification '{schema.classification}'"
                f" in registry for '{self._object_type}'"
            )

        # Use bisect_left to find the index of the closest version greater than or equal to the target version
        sorted_versions = sorted(self._loaders.keys())
        left_idx = bisect.bisect_left(sorted_versions, schema.version)

        # Check the candidate at the bisect_left index, then the one before it
        for candidate_idx in (left_idx, left_idx - 1):
            try:
                candidate = sorted_versions[candidate_idx]
                if candidate.major == schema.version.major:
                    # If the major versions match, return this adapter
                    return self._loaders[candidate]
            except IndexError:
                pass  # Ignore out-of-bounds indices

        # If no suitable adapter was found, raise ValueError
        raise ValueError(f"No loader found for {schema}")

    @staticmethod
    def get_registry(schema: ObjectSchema) -> DatasetLoaderRegistry | None:
        """Get the DatasetLoaderRegistry for the given ObjectSchema's classification.

        :param schema: The ObjectSchema to get the registry for.

        :return: The DatasetLoaderRegistry for the schema's classification, or None if no such registry exists.
        """
        return DatasetLoaderRegistry.__REGISTRIES.get(schema.classification)

    @staticmethod
    def register_loader(schema: ObjectSchema | str, loader: DatasetLoader) -> None:
        """Register an DatasetLoader for a specific ObjectSchema in the global registry.

        :param schema: The ObjectSchema (or its string ID) defining the classification and version for which the
            loader is registered.
        :param loader: The DatasetLoader to register for the given schema version.

        :raises ValueError: If the schema classification does not match the registry's object type,
            or if a loader is already registered for the given schema version.
        """
        if isinstance(schema, str):
            schema = ObjectSchema.from_id(schema)

        registry = DatasetLoaderRegistry.__REGISTRIES.setdefault(
            schema.classification, DatasetLoaderRegistry(schema.classification)
        )
        registry.register_version_loader(schema, loader)

    @staticmethod
    def resolve_loader(schema: ObjectSchema) -> DatasetLoader:
        """Resolve the most appropriate DatasetLoader for the given ObjectSchema from the global registry.

        :param schema: The ObjectSchema to resolve the loader for.

        :return: The most appropriate DatasetLoader for the given schema.

        :raises ValueError: If no registry exists for the schema's classification,
            or if no suitable loader is found for the given schema version.
        """
        registry = DatasetLoaderRegistry.get_registry(schema)
        if registry is None:
            raise ValueError(f"No registry found for schema classification '{schema.classification}'")
        else:
            return registry.resolve_version_loader(schema)


def __populate_registry() -> None:
    import warnings
    from importlib.resources import files

    from pydantic import TypeAdapter, ValidationError

    from evo.objects.exceptions import SchemaIDFormatError

    from ..types import DatasetLoaderSpec

    ta = TypeAdapter(dict[str, DatasetLoaderSpec])
    for target in files(__name__).iterdir():
        if not target.name.endswith(".json"):
            continue  # Skip non-YAML files

        try:
            specs_by_schema = ta.validate_json(target.read_text(encoding="utf-8"))
        except ValidationError as ve:
            warnings.warn(f"Skipping invalid loader spec file '{target}': {ve}")
            continue

        for schema_id, spec in specs_by_schema.items():
            try:
                schema = ObjectSchema.from_id(schema_id)
            except SchemaIDFormatError as sie:
                warnings.warn(f"Skipping invalid schema ID '{schema_id}' in file '{target}': {sie}")
                continue
            loader = DatasetLoader.from_spec(spec)

            try:
                DatasetLoaderRegistry.register_loader(schema, loader)
            except ValueError as ve:
                warnings.warn(f"Skipping duplicate loader for schema '{schema_id}' in file '{target}': {ve}")


__populate_registry()
del __populate_registry
