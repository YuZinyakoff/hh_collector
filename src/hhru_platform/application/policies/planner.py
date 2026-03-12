from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hhru_platform.domain.entities.crawl_run import CrawlRun


@dataclass(slots=True, frozen=True)
class PartitionPlanDefinition:
    partition_key: str
    params_json: dict[str, Any]


class SinglePartitionPlannerPolicyV1:
    def build(self, crawl_run: CrawlRun) -> list[PartitionPlanDefinition]:
        return [
            PartitionPlanDefinition(
                partition_key="global-default",
                params_json={
                    "planner_policy": "single_partition_v1",
                    "scope": "global",
                    "run_type": crawl_run.run_type,
                },
            )
        ]
