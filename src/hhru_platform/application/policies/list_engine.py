from __future__ import annotations

from dataclasses import dataclass

DEFAULT_SATURATION_PAGES_THRESHOLD_V1 = 100


@dataclass(slots=True, frozen=True)
class SaturationDecision:
    is_saturated: bool
    reason: str | None
    pages_total_expected: int | None
    pages_threshold: int


class PartitionSaturationPolicyV1:
    def __init__(
        self,
        *,
        pages_threshold: int = DEFAULT_SATURATION_PAGES_THRESHOLD_V1,
    ) -> None:
        if pages_threshold < 1:
            raise ValueError("pages_threshold must be greater than or equal to one")
        self._pages_threshold = pages_threshold

    @property
    def pages_threshold(self) -> int:
        return self._pages_threshold

    def decide(self, *, pages_total_expected: int | None) -> SaturationDecision:
        if pages_total_expected is None:
            return SaturationDecision(
                is_saturated=False,
                reason=None,
                pages_total_expected=None,
                pages_threshold=self._pages_threshold,
            )

        normalized_pages_total_expected = max(pages_total_expected, 0)
        if normalized_pages_total_expected >= self._pages_threshold:
            return SaturationDecision(
                is_saturated=True,
                reason=(
                    "pages_total_expected reached or exceeded the v1 saturation threshold: "
                    f"{normalized_pages_total_expected} >= {self._pages_threshold}"
                ),
                pages_total_expected=normalized_pages_total_expected,
                pages_threshold=self._pages_threshold,
            )

        return SaturationDecision(
            is_saturated=False,
            reason=None,
            pages_total_expected=normalized_pages_total_expected,
            pages_threshold=self._pages_threshold,
        )
