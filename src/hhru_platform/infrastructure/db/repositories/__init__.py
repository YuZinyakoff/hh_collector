"""Database repositories."""

from hhru_platform.infrastructure.db.repositories.api_request_log_repo import (
    SqlAlchemyApiRequestLogRepository,
)
from hhru_platform.infrastructure.db.repositories.area_repo import SqlAlchemyAreaRepository
from hhru_platform.infrastructure.db.repositories.crawl_partition_repo import (
    SqlAlchemyCrawlPartitionRepository,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import SqlAlchemyCrawlRunRepository
from hhru_platform.infrastructure.db.repositories.detail_fetch_attempt_repo import (
    SqlAlchemyDetailFetchAttemptRepository,
)
from hhru_platform.infrastructure.db.repositories.dictionary_store import SqlAlchemyDictionaryStore
from hhru_platform.infrastructure.db.repositories.dictionary_sync_run_repo import (
    SqlAlchemyDictionarySyncRunRepository,
)
from hhru_platform.infrastructure.db.repositories.employer_repo import (
    SqlAlchemyEmployerRepository,
)
from hhru_platform.infrastructure.db.repositories.professional_role_repo import (
    SqlAlchemyProfessionalRoleRepository,
)
from hhru_platform.infrastructure.db.repositories.raw_payload_repo import (
    SqlAlchemyRawApiPayloadRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_current_state_repo import (
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_professional_role_repo import (
    SqlAlchemyVacancyProfessionalRoleRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_repo import SqlAlchemyVacancyRepository
from hhru_platform.infrastructure.db.repositories.vacancy_seen_event_repo import (
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_snapshot_repo import (
    SqlAlchemyVacancySnapshotRepository,
)

__all__ = [
    "SqlAlchemyApiRequestLogRepository",
    "SqlAlchemyAreaRepository",
    "SqlAlchemyCrawlPartitionRepository",
    "SqlAlchemyCrawlRunRepository",
    "SqlAlchemyDetailFetchAttemptRepository",
    "SqlAlchemyDictionaryStore",
    "SqlAlchemyDictionarySyncRunRepository",
    "SqlAlchemyEmployerRepository",
    "SqlAlchemyProfessionalRoleRepository",
    "SqlAlchemyRawApiPayloadRepository",
    "SqlAlchemyVacancyCurrentStateRepository",
    "SqlAlchemyVacancyProfessionalRoleRepository",
    "SqlAlchemyVacancyRepository",
    "SqlAlchemyVacancySeenEventRepository",
    "SqlAlchemyVacancySnapshotRepository",
]
