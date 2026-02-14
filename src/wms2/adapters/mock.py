from typing import Any

from .base import CondorAdapter, CRICAdapter, DBSAdapter, ReqMgrAdapter, RucioAdapter


class MockCondorAdapter(CondorAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def submit_job(self, submit_file: str) -> tuple[str, str]:
        self.calls.append(("submit_job", (submit_file,), {}))
        return ("12345.0", "schedd.example.com")

    async def submit_dag(self, dag_file: str) -> tuple[str, str]:
        self.calls.append(("submit_dag", (dag_file,), {}))
        return ("12346.0", "schedd.example.com")

    async def query_job(self, schedd_name: str, cluster_id: str) -> dict[str, Any] | None:
        self.calls.append(("query_job", (schedd_name, cluster_id), {}))
        return {"ClusterId": cluster_id, "JobStatus": 2}

    async def check_job_completed(self, cluster_id: str, schedd_name: str) -> bool:
        self.calls.append(("check_job_completed", (cluster_id, schedd_name), {}))
        return True

    async def remove_job(self, schedd_name: str, cluster_id: str) -> None:
        self.calls.append(("remove_job", (schedd_name, cluster_id), {}))

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
        }


class MockDBSAdapter(DBSAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def get_files(self, dataset: str, limit: int = 0) -> list[dict[str, Any]]:
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


class MockRucioAdapter(RucioAdapter):
    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    async def get_replicas(self, dataset: str) -> list[dict[str, Any]]:
        self.calls.append(("get_replicas", (dataset,), {}))
        return [{"rse": "T1_US_FNAL_Disk", "state": "AVAILABLE"}]

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
