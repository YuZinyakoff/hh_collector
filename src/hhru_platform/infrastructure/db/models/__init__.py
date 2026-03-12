from hhru_platform.infrastructure.db.models.api_request_log import ApiRequestLog
from hhru_platform.infrastructure.db.models.area import Area
from hhru_platform.infrastructure.db.models.crawl_partition import CrawlPartition
from hhru_platform.infrastructure.db.models.crawl_run import CrawlRun
from hhru_platform.infrastructure.db.models.detail_fetch_attempt import DetailFetchAttempt
from hhru_platform.infrastructure.db.models.dictionary_sync_run import DictionarySyncRun
from hhru_platform.infrastructure.db.models.employer import Employer
from hhru_platform.infrastructure.db.models.professional_role import ProfessionalRole
from hhru_platform.infrastructure.db.models.raw_api_payload import RawApiPayload
from hhru_platform.infrastructure.db.models.vacancy import Vacancy
from hhru_platform.infrastructure.db.models.vacancy_current_state import VacancyCurrentState
from hhru_platform.infrastructure.db.models.vacancy_professional_role import VacancyProfessionalRole
from hhru_platform.infrastructure.db.models.vacancy_seen_event import VacancySeenEvent
from hhru_platform.infrastructure.db.models.vacancy_snapshot import VacancySnapshot

__all__ = [
    "ApiRequestLog",
    "Area",
    "CrawlPartition",
    "CrawlRun",
    "DictionarySyncRun",
    "DetailFetchAttempt",
    "Employer",
    "ProfessionalRole",
    "RawApiPayload",
    "Vacancy",
    "VacancyCurrentState",
    "VacancyProfessionalRole",
    "VacancySeenEvent",
    "VacancySnapshot",
]
