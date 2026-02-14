from abc import ABC, abstractmethod
from typing import Any


class CondorAdapter(ABC):
    @abstractmethod
    async def submit_job(self, submit_file: str) -> tuple[str, str]:
        """Submit a job. Returns (cluster_id, schedd_name)."""

    @abstractmethod
    async def submit_dag(self, dag_file: str) -> tuple[str, str]:
        """Submit a DAG. Returns (dagman_cluster_id, schedd_name)."""

    @abstractmethod
    async def query_job(self, schedd_name: str, cluster_id: str) -> dict[str, Any] | None:
        """Query job status. Returns None if job not found."""

    @abstractmethod
    async def check_job_completed(self, cluster_id: str, schedd_name: str) -> bool:
        """Check if a job has completed."""

    @abstractmethod
    async def remove_job(self, schedd_name: str, cluster_id: str) -> None:
        """Remove (condor_rm) a job."""

    @abstractmethod
    async def ping_schedd(self, schedd_name: str) -> bool:
        """Check if a schedd is reachable."""


class ReqMgrAdapter(ABC):
    @abstractmethod
    async def get_request(self, request_name: str) -> dict[str, Any]:
        """Fetch a request from ReqMgr2."""


class DBSAdapter(ABC):
    @abstractmethod
    async def get_files(self, dataset: str, limit: int = 0) -> list[dict[str, Any]]:
        """Get files for a dataset from DBS."""

    @abstractmethod
    async def inject_dataset(self, dataset_info: dict[str, Any]) -> None:
        """Register a dataset in DBS."""

    @abstractmethod
    async def invalidate_dataset(self, dataset_name: str, reason: str) -> None:
        """Invalidate a dataset in DBS."""


class RucioAdapter(ABC):
    @abstractmethod
    async def get_replicas(self, dataset: str) -> list[dict[str, Any]]:
        """Get replica locations for a dataset."""

    @abstractmethod
    async def create_rule(self, dataset: str, destination: str, **kwargs: Any) -> str:
        """Create a Rucio replication rule. Returns rule_id."""

    @abstractmethod
    async def get_rule_status(self, rule_id: str) -> dict[str, Any]:
        """Get status of a Rucio rule."""

    @abstractmethod
    async def delete_rule(self, rule_id: str) -> None:
        """Delete a Rucio rule."""


class CRICAdapter(ABC):
    @abstractmethod
    async def get_sites(self) -> list[dict[str, Any]]:
        """Get site information from CRIC."""
