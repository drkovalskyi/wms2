from typing import Any

from .base import CondorAdapter, CRICAdapter, DBSAdapter, ReqMgrAdapter, RucioAdapter


class MockCondorAdapter(CondorAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []
        self._next_cluster_id = 12345
        self.completed_jobs: set[str] = set()
        self.removed_jobs: set[str] = set()

    async def submit_job(self, submit_file: str) -> tuple[str, str]:
        self.calls.append(("submit_job", (submit_file,), {}))
        cluster_id = str(self._next_cluster_id)
        self._next_cluster_id += 1
        return (cluster_id, "schedd.example.com")

    async def submit_dag(self, dag_file: str) -> tuple[str, str]:
        self.calls.append(("submit_dag", (dag_file,), {}))
        cluster_id = str(self._next_cluster_id)
        self._next_cluster_id += 1
        return (cluster_id, "schedd.example.com")

    async def query_job(self, schedd_name: str, cluster_id: str) -> dict[str, Any] | None:
        self.calls.append(("query_job", (schedd_name, cluster_id), {}))
        if cluster_id in self.completed_jobs or cluster_id in self.removed_jobs:
            return None
        return {"ClusterId": cluster_id, "JobStatus": 2}

    async def check_job_completed(self, cluster_id: str, schedd_name: str) -> bool:
        self.calls.append(("check_job_completed", (cluster_id, schedd_name), {}))
        return cluster_id in self.completed_jobs

    async def remove_job(self, schedd_name: str, cluster_id: str) -> None:
        self.calls.append(("remove_job", (schedd_name, cluster_id), {}))
        self.removed_jobs.add(cluster_id)

    async def ping_schedd(self, schedd_name: str) -> bool:
        self.calls.append(("ping_schedd", (schedd_name,), {}))
        return True


class MockReqMgrAdapter(ReqMgrAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def get_request(self, request_name: str) -> dict[str, Any]:
        self.calls.append(("get_request", (request_name,), {}))
        return {
            "RequestName": request_name,
            "InputDataset": "/TestPrimary/TestProcessed/RECO",
            "SplittingAlgo": "FileBased",
            "SplittingParams": {"files_per_job": 10},
            "SandboxUrl": "https://example.com/sandbox.tar.gz",
            "Requestor": "testuser",
            "Campaign": "TestCampaign",
            "Priority": 100000,
        }

    async def get_assigned_requests(self, agent_name: str) -> list[dict[str, Any]]:
        self.calls.append(("get_assigned_requests", (agent_name,), {}))
        return [
            {
                "RequestName": "test-request-001",
                "InputDataset": "/TestPrimary/TestProcessed/RECO",
                "SplittingAlgo": "FileBased",
                "SplittingParams": {"files_per_job": 10},
                "SandboxUrl": "https://example.com/sandbox.tar.gz",
                "Requestor": "testuser",
                "Campaign": "TestCampaign",
                "Priority": 100000,
            }
        ]


class MockDBSAdapter(DBSAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def get_files(
        self,
        dataset: str,
        limit: int = 0,
        run_whitelist: list[int] | None = None,
        lumi_mask: dict[str, list[list[int]]] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("get_files", (dataset,), {"limit": limit}))
        return [
            {
                "logical_file_name": f"/store/data/file_{i}.root",
                "file_size": 1_000_000_000,
                "event_count": 10000,
            }
            for i in range(min(limit, 5) if limit else 5)
        ]

    async def inject_dataset(self, dataset_info: dict[str, Any]) -> None:
        self.calls.append(("inject_dataset", (dataset_info,), {}))

    async def invalidate_dataset(self, dataset_name: str, reason: str) -> None:
        self.calls.append(("invalidate_dataset", (dataset_name, reason), {}))

    async def open_block(self, dataset_name: str, block_index: int) -> str:
        self.calls.append(("open_block", (dataset_name, block_index), {}))
        return f"{dataset_name}#block_{block_index}"

    async def register_files(self, block_name: str, files: list[dict]) -> None:
        self.calls.append(("register_files", (block_name, files), {}))

    async def close_block(self, block_name: str) -> None:
        self.calls.append(("close_block", (block_name,), {}))

    async def invalidate_block(self, block_name: str) -> None:
        self.calls.append(("invalidate_block", (block_name,), {}))


class MockRucioAdapter(RucioAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def get_replicas(self, lfns: list[str]) -> dict[str, list[str]]:
        self.calls.append(("get_replicas", (lfns,), {}))
        return {lfn: ["T1_US_FNAL", "T2_US_MIT"] for lfn in lfns}

    async def create_rule(self, dataset: str, destination: str, **kwargs: Any) -> str:
        self.calls.append(("create_rule", (dataset, destination), kwargs))
        return "rule-id-12345"

    async def get_rule_status(self, rule_id: str) -> dict[str, Any]:
        self.calls.append(("get_rule_status", (rule_id,), {}))
        return {"state": "OK", "locks_ok_cnt": 100}

    async def delete_rule(self, rule_id: str) -> None:
        self.calls.append(("delete_rule", (rule_id,), {}))


class MockCRICAdapter(CRICAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def get_sites(self) -> list[dict[str, Any]]:
        self.calls.append(("get_sites", (), {}))
        return [
            {
                "name": "T1_US_FNAL",
                "total_slots": 50000,
                "status": "enabled",
                "schedd_name": "schedd1.fnal.gov",
                "collector_host": "collector.fnal.gov",
                "storage_element": "se.fnal.gov",
                "storage_path": "/store/",
            },
            {
                "name": "T2_US_MIT",
                "total_slots": 10000,
                "status": "enabled",
                "schedd_name": "schedd1.mit.edu",
                "collector_host": "collector.mit.edu",
                "storage_element": "se.mit.edu",
                "storage_path": "/store/",
            },
        ]
