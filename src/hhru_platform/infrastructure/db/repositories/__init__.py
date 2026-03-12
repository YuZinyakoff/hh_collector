"""Database repositories."""

from hhru_platform.infrastructure.db.repositories.api_request_log_repo import (
    SqlAlchemyApiRequestLogRepository,
)
from hhru_platform.infrastructure.db.repositories.area_repo import SqlAlchemyAreaRepository
from hhru_platform.infrastructure.db.repositories.crawl_partition_repo import (
    SqlAlchemyCrawlPartitionRepository,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import SqlAlchemyCrawlRunRepository
from hhru_platform.infrastructure.db.repositories.dictionary_store import SqlAlchemyDictionaryStore
from hhru_platform.infrastructure.db.repositories.dictionary_sync_run_repo import (
    SqlAlchemyDictionarySyncRunRepository,
)
from hhru_platform.infrastructure.db.repositories.professional_role_repo import (
    SqlAlchemyProfessionalRoleRepository,
)
from hhru_platform.infrastructure.db.repositories.raw_payload_repo import (
    SqlAlchemyRawApiPayloadRepository,
)

__all__ = [
    "SqlAlchemyApiRequestLogRepository",
    "SqlAlchemyAreaRepository",
    "SqlAlchemyCrawlPartitionRepository",
    "SqlAlchemyCrawlRunRepository",
    "SqlAlchemyDictionaryStore",
    "SqlAlchemyDictionarySyncRunRepository",
    "SqlAlchemyProfessionalRoleRepository",
    "SqlAlchemyRawApiPayloadRepository",
]
