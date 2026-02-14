import logging

from wms2.config import Settings
from wms2.db.repository import Repository

logger = logging.getLogger(__name__)


class AdmissionController:
    """
    Controls the rate of DAG submissions to prevent resource starvation.
    Priority-ordered FIFO with a concurrency limit.

    Called by the Request Lifecycle Manager — not an independent loop.
    """

    def __init__(self, repository: Repository, settings: Settings):
        self.db = repository
        self.max_active_dags = settings.max_active_dags

    async def has_capacity(self) -> bool:
        """Check if there is capacity to admit another DAG."""
        active_count = await self.db.count_active_dags()
        return active_count < self.max_active_dags

    async def get_next_pending(self):
        """Get the next request to admit, ordered by priority then age."""
        pending = await self.db.get_queued_requests(limit=1)
        return pending[0] if pending else None

    async def get_queue_status(self) -> dict:
        """Return current queue status for the API."""
        active_count = await self.db.count_active_dags()
        pending = await self.db.get_queued_requests(limit=100)
        return {
            "active_dags": active_count,
            "max_active_dags": self.max_active_dags,
            "capacity_available": active_count < self.max_active_dags,
            "queued_requests": len(pending),
            "next_pending": pending[0].request_name if pending else None,
        }
