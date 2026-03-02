from abc import ABC, abstractmethod
from typing import Any


class CondorAdapter(ABC):
    @abstractmethod
    async def submit_job(self, submit_file: str) -> tuple[str, str]:
        """Submit a job. Returns (cluster_id, schedd_name)."""

    @abstractmethod
    async def submit_dag(self, dag_file: str, force: bool = False) -> tuple[str, str]:
        """Submit a DAG. Returns (dagman_cluster_id, schedd_name).

        Args:
            force: If True, pass Force option to from_dag() to allow
                   resubmission over existing lock/output files (rescue DAGs).
        """

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

    async def query_dag_jobs(self, cluster_id: str) -> list[dict] | None:
        """Query per-job details for all payload jobs under a DAGMan hierarchy.

        Returns list of dicts with keys: name, status, wall_time, memory_mb,
        cpu_user, cpu_sys, cpu_efficiency, cpus, site.
        Returns None if not supported by this adapter.
        """
        return None

    async def count_dag_jobs(self, cluster_id: str) -> dict[str, int] | None:
        """Count jobs by status for all payload jobs under a DAGMan hierarchy.

        Returns dict with keys: idle, running, done, held, failed, total.
        Returns None if not supported by this adapter.
        """
        return None


class ReqMgrAdapter(ABC):
    @abstractmethod
    async def get_request(self, request_name: str) -> dict[str, Any]:
        """Fetch a request from ReqMgr2."""

    @abstractmethod
    async def get_assigned_requests(self, agent_name: str) -> list[dict[str, Any]]:
        """Fetch requests assigned to the given agent."""


class DBSAdapter(ABC):
    @abstractmethod
    async def get_files(
        self,
        dataset: str,
        limit: int = 0,
        run_whitelist: list[int] | None = None,
        lumi_mask: dict[str, list[list[int]]] | None = None,
    ) -> list[dict[str, Any]]:
        """Get files for a dataset from DBS."""

    @abstractmethod
    async def inject_dataset(self, dataset_info: dict[str, Any]) -> None:
        """Register a dataset in DBS."""

    @abstractmethod
    async def invalidate_dataset(self, dataset_name: str, reason: str) -> None:
        """Invalidate a dataset in DBS."""

    @abstractmethod
    async def open_block(self, dataset_name: str, block_index: int) -> str:
        """Create/open a new DBS block. Returns block_name."""

    @abstractmethod
    async def register_files(self, block_name: str, files: list[dict]) -> None:
        """Register output files within an open DBS block."""

    @abstractmethod
    async def close_block(self, block_name: str) -> None:
        """Close a DBS block (no more files will be added)."""

    @abstractmethod
    async def invalidate_block(self, block_name: str) -> None:
        """Invalidate a DBS block and its files."""

    async def list_files(self, dataset: str, limit: int = 1) -> list[dict[str, Any]]:
        """List files in a dataset (minimal detail). Used for LFN base lookup.

        Default implementation delegates to get_files with the given limit.
        """
        return await self.get_files(dataset, limit=limit)


class RucioAdapter(ABC):
    @abstractmethod
    async def get_replicas(self, lfns: list[str]) -> dict[str, list[str]]:
        """Get replica locations per LFN. Returns {lfn: [site_name, ...]}."""

    @abstractmethod
    async def create_rule(self, dataset: str, destination: str, **kwargs: Any) -> str:
        """Create a Rucio replication rule. Returns rule_id."""

    @abstractmethod
    async def get_rule_status(self, rule_id: str) -> dict[str, Any]:
        """Get status of a Rucio rule."""

    @abstractmethod
    async def delete_rule(self, rule_id: str) -> None:
        """Delete a Rucio rule."""

    @abstractmethod
    async def get_available_pileup_files(self, dataset: str,
                                         preferred_rses: list[str] | None = None) -> list[str]:
        """Get LFNs with on-disk replicas for a pileup dataset.

        If preferred_rses is non-empty, only files at those RSEs are returned.
        """


class CRICAdapter(ABC):
    @abstractmethod
    async def get_sites(self) -> list[dict[str, Any]]:
        """Get site information from CRIC."""
