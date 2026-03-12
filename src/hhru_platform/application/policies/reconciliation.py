from __future__ import annotations

from uuid import UUID

from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentState,
    VacancyCurrentStateReconciliationUpdate,
)


class MissingRunsReconciliationPolicyV1:
    def __init__(self, *, inactive_after_missing_runs: int = 2) -> None:
        if inactive_after_missing_runs < 1:
            raise ValueError("inactive_after_missing_runs must be greater than zero")
        self._inactive_after_missing_runs = inactive_after_missing_runs

    def decide(
        self,
        *,
        vacancy_state: VacancyCurrentState,
        seen_in_run: bool,
        crawl_run_id: UUID,
    ) -> VacancyCurrentStateReconciliationUpdate:
        if seen_in_run:
            return VacancyCurrentStateReconciliationUpdate(
                vacancy_id=vacancy_state.vacancy_id,
                consecutive_missing_runs=0,
                is_probably_inactive=False,
                last_seen_run_id=crawl_run_id,
            )

        consecutive_missing_runs = vacancy_state.consecutive_missing_runs + 1
        return VacancyCurrentStateReconciliationUpdate(
            vacancy_id=vacancy_state.vacancy_id,
            consecutive_missing_runs=consecutive_missing_runs,
            is_probably_inactive=(
                consecutive_missing_runs >= self._inactive_after_missing_runs
            ),
            last_seen_run_id=vacancy_state.last_seen_run_id,
        )
