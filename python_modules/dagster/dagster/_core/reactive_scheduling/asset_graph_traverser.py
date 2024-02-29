import itertools
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, AbstractSet, List, Optional, Sequence, Set, Union

import pendulum

from dagster import (
    _check as check,
)
from dagster._core.definitions.asset_graph_subset import AssetGraphSubset
from dagster._core.definitions.asset_subset import AssetSubset, ValidAssetSubset
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.internal_asset_graph import InternalAssetGraph
from dagster._core.definitions.partition import (
    DefaultPartitionsSubset,
    PartitionsDefinition,
    PartitionsSubset,
)
from dagster._core.definitions.time_window_partitions import (
    TimeWindow,
    TimeWindowPartitionsDefinition,
    TimeWindowPartitionsSubset,
)
from dagster._utils.caching_instance_queryer import CachingInstanceQueryer

if TYPE_CHECKING:
    from dagster._core.definitions.data_version import CachingStaleStatusResolver
    from dagster._core.definitions.definitions_class import Definitions
    from dagster._core.instance import DagsterInstance
    from dagster._utils.caching_instance_queryer import CachingInstanceQueryer


def graph_subset_from_valid_subset(asset_subset: ValidAssetSubset) -> AssetGraphSubset:
    if asset_subset.is_partitioned:
        return AssetGraphSubset(
            partitions_subsets_by_asset_key={asset_subset.asset_key: asset_subset.subset_value}
        )
    else:
        return AssetGraphSubset(
            non_partitioned_asset_keys=(
                {asset_subset.asset_key} if asset_subset.bool_value else set()
            )
        )


class AssetSubsetFactory:
    @classmethod
    def unpartitioned(
        cls, asset_graph: InternalAssetGraph, asset_key: AssetKey
    ) -> ValidAssetSubset:
        return cls._make_valid(asset_graph, asset_key=asset_key, value=True)

    @classmethod
    def from_partition_keys(
        cls, asset_graph: InternalAssetGraph, asset_key: AssetKey, partition_keys: AbstractSet[str]
    ) -> ValidAssetSubset:
        return cls._make_valid(
            asset_graph=asset_graph,
            asset_key=asset_key,
            value=DefaultPartitionsSubset(partition_keys),
        )

    @classmethod
    def from_time_window(
        cls, asset_graph: InternalAssetGraph, asset_key: AssetKey, time_window: TimeWindow
    ):
        partitions_def = check.not_none(asset_graph.get_assets_def(asset_key).partitions_def)
        check.inst(partitions_def, TimeWindowPartitionsDefinition)
        assert isinstance(partitions_def, TimeWindowPartitionsDefinition)
        return cls._make_valid(
            asset_graph=asset_graph,
            asset_key=asset_key,
            value=TimeWindowPartitionsSubset(
                partitions_def=partitions_def,
                num_partitions=None,
                included_time_windows=[time_window],
            ),
        )

    @staticmethod
    def _make_valid(
        asset_graph: InternalAssetGraph, asset_key: AssetKey, value: Union[bool, PartitionsSubset]
    ) -> ValidAssetSubset:
        return AssetSubset(asset_key, value).as_valid(
            asset_graph.get_assets_def(asset_key).partitions_def
        )


class AssetGraphTraverser:
    def __init__(
        self,
        stale_resolver: "CachingStaleStatusResolver",
        current_dt: datetime,
    ):
        assert isinstance(stale_resolver.asset_graph, InternalAssetGraph)
        self.asset_graph = stale_resolver.asset_graph
        self.stale_resolver = stale_resolver
        self.current_dt = current_dt

    @property
    def queryer(self) -> "CachingInstanceQueryer":
        return self.stale_resolver.instance_queryer

    @staticmethod
    def for_test(
        defs: "Definitions",
        instance: Optional["DagsterInstance"] = None,
        current_dt: Optional[datetime] = None,
    ):
        from dagster._core.definitions.data_version import CachingStaleStatusResolver
        from dagster._core.instance import DagsterInstance

        return AssetGraphTraverser(
            CachingStaleStatusResolver(
                instance=instance or DagsterInstance.ephemeral(),
                asset_graph=defs.get_repository_def().asset_graph,
            ),
            current_dt or pendulum.now(),
        )

    def get_partitions_def(self, asset_key: AssetKey) -> Optional[PartitionsDefinition]:
        return self.asset_graph.get_assets_def(asset_key).partitions_def

    def _validate(self, asset_subset: AssetSubset) -> ValidAssetSubset:
        return asset_subset.as_valid(self.get_partitions_def(asset_subset.asset_key))

    def parent_asset_subset(
        self, parent_asset_key: AssetKey, asset_subset: ValidAssetSubset
    ) -> ValidAssetSubset:
        return self.asset_graph.get_parent_asset_subset(
            dynamic_partitions_store=self.queryer,
            parent_asset_key=parent_asset_key,
            child_asset_subset=asset_subset,
            current_time=self.current_dt,
        )

    def child_asset_subset(
        self, child_asset_key: AssetKey, asset_subset: ValidAssetSubset
    ) -> ValidAssetSubset:
        return self.asset_graph.get_child_asset_subset(
            dynamic_partitions_store=self.queryer,
            child_asset_key=child_asset_key,
            parent_asset_subset=asset_subset,
            current_time=self.current_dt,
        )

    def create_upstream_asset_graph_subset(
        self, starting_subset: ValidAssetSubset
    ) -> AssetGraphSubset:
        ag_subset = graph_subset_from_valid_subset(starting_subset)

        def _ascend(current_subset: ValidAssetSubset) -> None:
            nonlocal ag_subset
            for parent_key in self.asset_graph.get_parents(current_subset.asset_key):
                parent_subset = self.parent_asset_subset(parent_key, starting_subset)
                # TODO can check to see if parent is already in subset and early return
                ag_subset |= graph_subset_from_valid_subset(parent_subset)
                _ascend(parent_subset)

        _ascend(starting_subset)
        return ag_subset

    def create_partition_space_upstream_of_subsets(
        self, starting_subset: ValidAssetSubset
    ) -> "PartitionSpace":
        return PartitionSpace(
            self.asset_graph, self.create_upstream_asset_graph_subset(starting_subset)
        )


# TODO: make thie space-efficient with __slots__ or something
class PartitionSpace:
    """Represents a slice of partitions in the context of an asset graph."""

    def __init__(self, asset_graph: "InternalAssetGraph", asset_graph_subset: AssetGraphSubset):
        self.asset_graph = asset_graph
        self.asset_graph_subset = asset_graph_subset

    @staticmethod
    def empty(asset_graph: "InternalAssetGraph") -> "PartitionSpace":
        return PartitionSpace(asset_graph, AssetGraphSubset())

    def for_keys(self, asset_keys: AbstractSet[AssetKey]) -> "PartitionSpace":
        return PartitionSpace(
            self.asset_graph, self.asset_graph_subset.filter_asset_keys(asset_keys)
        )

    @cached_property
    def asset_keys(self) -> Set[AssetKey]:
        return set(self.asset_graph_subset.asset_keys)

    @cached_property
    def root_asset_keys(self) -> Set[AssetKey]:
        roots = set()
        subset_asset_keys = self.asset_graph_subset.asset_keys
        for asset_key in subset_asset_keys:
            if not any(
                parent_key in subset_asset_keys
                for parent_key in self.asset_graph.get_parents(asset_key)
            ):
                roots.add(asset_key)
        return roots

    @property
    def toposort_asset_keys(self) -> List[AssetKey]:
        return list(itertools.chain(*self.toposort_asset_levels))

    @cached_property
    def toposort_asset_levels(self) -> Sequence[AbstractSet[AssetKey]]:
        subset_asset_keys = self.asset_graph_subset.asset_keys
        filtered_levels = []
        for level in self.asset_graph.toposort_asset_keys():
            filtered_level = {key for key in level if key in subset_asset_keys}
            if filtered_level:
                filtered_levels.append(filtered_level)
        return filtered_levels

    def with_partition_space(self, other: "PartitionSpace") -> "PartitionSpace":
        return PartitionSpace(self.asset_graph, self.asset_graph_subset | other.asset_graph_subset)

    def with_asset_subset(self, asset_subset: ValidAssetSubset) -> "PartitionSpace":
        return PartitionSpace(
            self.asset_graph, self.asset_graph_subset | graph_subset_from_valid_subset(asset_subset)
        )

    def get_asset_subset(self, asset_key: AssetKey) -> ValidAssetSubset:
        assets_def = self.asset_graph.get_assets_def(asset_key)
        if assets_def.partitions_def is None:
            return AssetSubset(
                asset_key=asset_key,
                value=asset_key in self.asset_graph_subset.non_partitioned_asset_keys,
            ).as_valid(assets_def.partitions_def)
        else:
            return AssetSubset(
                asset_key=asset_key,
                value=self.asset_graph_subset.partitions_subsets_by_asset_key.get(
                    asset_key, assets_def.partitions_def.empty_subset()
                ),
            ).as_valid(assets_def.partitions_def)
