import sys
import uuid
from typing import Any, Mapping, overload

from evo.common import APIConnector, Environment, IAuthorizer, ICache, ITransport
from evo.objects import DownloadedObject, ObjectAPIClient, ObjectReference
from evo.objects.utils import ObjectDataClient

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class EvoContext:
    """Context for Evo APIs.

    This contains the following context:
    - Transport and authorizer, required to connect to Evo APIs.
    - Optional cache, which will be used for caching downloads of data, and other temporary data.
    - Hub, organization, workspace, and folder, to use when performing operations that require these parameters.
    """

    @overload
    def __init__(
        self,
        transport: ITransport,
        authorizer: IAuthorizer,
        additional_headers: Mapping[str, Any] | None = None,
        cache: ICache | None = None,
        hub_url: str | None = None,
        org_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        folder: str | None = None,
    ): ...

    @overload
    def __init__(
        self,
        connector: APIConnector,
        cache: ICache | None = None,
        hub_url: str | None = None,
        org_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        folder: str | None = None,
    ): ...

    def __init__(
        self,
        transport: ITransport | None = None,
        authorizer: IAuthorizer | None = None,
        connector: APIConnector | None = None,
        additional_headers: Mapping[str, Any] | None = None,
        cache: ICache | None = None,
        hub_url: str | None = None,
        org_id: uuid.UUID | None = None,
        workspace_id: uuid.UUID | None = None,
        folder: str | None = None,
    ):
        if connector is not None:
            if transport is not None or authorizer is not None or additional_headers is not None:
                raise ValueError(
                    "If 'connector' is provided, 'transport', 'authorizer', and 'additional_headers' must not be provided."
                )
            self._connector = connector
            if hub_url is not None:
                hub_url = connector.base_url
            elif hub_url != connector.base_url:
                raise ValueError("If 'connector' is provided, its base_url must match the provided 'hub'.")

            self._transport = None
            self._authorizer = None
            self._additional_headers = None
        else:
            if transport is None or authorizer is None:
                raise ValueError("'transport' and 'authorizer' must be provided if 'connector' is not provided.")
            self._connector = None
            self._transport = transport
            self._authorizer = authorizer
            self._additional_headers = additional_headers
        self._cache = cache
        self._hub_url = hub_url
        self._org_id = org_id
        self._workspace_id = workspace_id
        self._folder = folder

    def _with(self, **kwargs: Any) -> Self:
        """Create a copy of this context with the provided changes."""
        if self._connector is not None:
            return EvoContext(
                connector=self._connector,
                cache=kwargs.get("cache", self._cache),
                hub_url=kwargs.get("hub_url", self._hub_url),
                org_id=kwargs.get("org_id", self._org_id),
                workspace_id=kwargs.get("workspace_id", self._workspace_id),
                folder=kwargs.get("folder", self._folder),
            )
        else:
            return EvoContext(
                transport=self._transport,
                authorizer=self._authorizer,
                additional_headers=self._additional_headers,
                cache=kwargs.get("cache", self._cache),
                hub_url=kwargs.get("hub_url", self._hub_url),
                org_id=kwargs.get("org_id", self._org_id),
                workspace_id=kwargs.get("workspace_id", self._workspace_id),
                folder=kwargs.get("folder", self._folder),
            )

    def with_cache(self, cache: ICache) -> Self:
        """Create a copy of this context with the provided cache."""
        return self._with(cache=cache)

    def with_org(self, org_id: uuid.UUID, hub_url: str | None = None) -> Self:
        """Create a copy of this context with the provided organization.

        If `hub_url` is not provided, the current hub URL will be reused.
        """
        if hub_url is None and self._hub_url is None:
            raise ValueError("Can't set organization without a hub URL. Please provide the hub URL.")
        return self._with(
            org_id=org_id,
            hub_url=hub_url if hub_url is not None else self._hub_url,
            workspace=None,
            folder=None,
        )

    def with_workspace(self, workspace: uuid.UUID, org_id: uuid.UUID | None = None, hub_url: str | None = None) -> Self:
        """Create a copy of this context with the provided workspace.

        If 'org_id' or 'hub_url` are not provided, the current values will be reused.
        """
        if hub_url is None and self._hub_url is None:
            raise ValueError("Can't set workspace without a hub URL. Please provide the hub URL.")
        if org_id is None and self._org_id is None:
            raise ValueError("Can't set workspace without an organization. Please provide the organization ID.")
        return self._with(
            workspace=workspace,
            org_id=org_id if org_id is not None else self._org_id,
            hub_url=hub_url if hub_url is not None else self._hub_url,
            folder=None,
        )

    def get_connector(self) -> APIConnector:
        if self._connector is not None:
            return self._connector
        hub_url = self._hub_url
        if hub_url is None:
            raise ValueError("Can't determine hub URL for connector. Context must have the hub URL set.")
        return APIConnector(
            base_url=hub_url,
            transport=self._transport,
            authorizer=self._authorizer,
            additional_headers=self._additional_headers,
        )

    def get_environment(self) -> Environment:
        if self._hub_url is None:
            raise ValueError("Can't determine hub URL for the environment. Context must have a hub URL set.")
        if self._org_id is None:
            raise ValueError("Can't determine organization for environment. Context must have an organization ID set.")
        if self._workspace_id is None:
            raise ValueError("Can't determine workspace for environment. Context must have a workspace ID set.")
        return Environment(
            hub_url=self._hub_url,
            org_id=self._org_id,
            workspace_id=self._workspace_id,
        )

    def with_folder(self, folder: str) -> Self:
        """Create a copy of this context with the provided folder."""
        return self._with(folder=folder)

    # TODO: Decide where the following methods should live
    # The advantage of being here is that this provides a good abstraction level between the low level clients and the
    # high level objects. Which, we could use to provide a MockEvoContext, or other similar utilities. However, these
    # Geoscience Object specific methods may not belong in a general EvoContext.

    def check_object_reference(self, reference: ObjectReference) -> None:
        """Check that the provided object reference is within the current context. The folder is not checked.

        i.e. that the hub URL, organization, and workspace match.
        """
        if reference.workspace_id != self._workspace_id:
            raise ValueError(
                f"Object's workspace '{reference.workspace_id}' does not match the workspace '{self._workspace_id}'."
            )
        if reference.org_id != self._org_id:
            raise ValueError(
                f"Object's organization '{reference.org_id}' does not match the organization '{self._org_id}'."
            )
        if reference.hub_url != self._hub_url:
            raise ValueError(
                f"Object's hub URL '{reference.hub_url}' does not match the context's hub URL '{self._hub_url}'."
            )

    def get_data_client(self) -> ObjectDataClient:
        connector = self.get_connector()
        environment = self.get_environment()
        return ObjectDataClient(environment, connector, cache=self._cache)

    async def create_geoscience_object(self, object_dict: dict[str, Any]) -> DownloadedObject:
        connector = self.get_connector()
        environment = self.get_environment()

        client = ObjectAPIClient(environment, connector, cache=self._cache)

        name = object_dict["name"]
        parent = self._folder

        # TODO Smarter path handling, i.e. URL encode the name to handle arbitrary characters
        path = parent + name + ".json" if parent else name + ".json"
        metadata = await client.create_geoscience_object(
            path=path,
            object_dict=object_dict,
        )

        # Need to perform a GET request to get the URLs required to download the data
        return await DownloadedObject.from_reference(connector, metadata.url, self._cache)

    async def replace_geoscience_object(
        self, reference: ObjectReference, object_dict: dict[str, Any]
    ) -> DownloadedObject:
        self.check_object_reference(reference)
        connector = self.get_connector()
        environment = self.get_environment()
        client = ObjectAPIClient(environment, connector, cache=self._cache)

        reference = ObjectReference(reference)
        if reference.object_id is not None:
            object_dict["uuid"] = reference.object_id
        else:
            # Need to perform a GET request to get the existing object's UUID
            existing_obj = await client.download_object_by_path(reference.object_path)
            object_dict["uuid"] = existing_obj.metadata.id

        metadata = await client.update_geoscience_object(object_dict)

        # Need to perform a GET request to get the URLs required to download the data
        return await DownloadedObject.from_reference(connector, metadata.url, self._cache)

    async def download_geoscience_object(self, reference: ObjectReference) -> DownloadedObject:
        self.check_object_reference(reference)
        return await DownloadedObject.from_reference(
            connector=self.get_connector(),
            reference=reference,
            cache=self._cache,
        )

    @classmethod
    def from_downloaded_object(
        cls,
        downloaded_object: DownloadedObject,
    ) -> "EvoContext":
        return EvoContext.from_environment(
            connector=downloaded_object.connector,
            environment=downloaded_object.metadata.environment,
            cache=downloaded_object.cache,
        )

    @classmethod
    def from_environment(
        cls,
        connector: APIConnector,
        environment: Environment,
        cache: ICache | None = None,
    ) -> "EvoContext":
        return cls(
            connector=connector,
            cache=cache,
            hub_url=environment.hub_url,
            org_id=environment.org_id,
            workspace_id=environment.workspace_id,
        )
