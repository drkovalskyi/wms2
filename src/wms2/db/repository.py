from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .tables import (
    DAGHistoryRow,
    DAGRow,
    OutputDatasetRow,
    RequestRow,
    SiteRow,
    WorkflowRow,
)


class Repository:
    def __init__(self, session: AsyncSession):
        self.session = session

    # ── Requests ────────────────────────────────────────────────

    async def create_request(self, **kwargs: Any) -> RequestRow:
        row = RequestRow(**kwargs)
        self.session.add(row)
        await self.session.flush()
        return row

    async def get_request(self, request_name: str) -> RequestRow | None:
        result = await self.session.execute(
            select(RequestRow).where(RequestRow.request_name == request_name)
        )
        return result.scalar_one_or_none()

    async def list_requests(
        self,
        status: str | None = None,
        campaign: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RequestRow]:
        stmt = select(RequestRow)
        if status:
            stmt = stmt.where(RequestRow.status == status)
        if campaign:
            stmt = stmt.where(RequestRow.campaign == campaign)
        stmt = stmt.order_by(RequestRow.created_at.desc()).limit(limit).offset(offset)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def update_request(self, request_name: str, **kwargs: Any) -> RequestRow | None:
        kwargs["updated_at"] = datetime.now(timezone.utc)
        stmt = (
            update(RequestRow)
            .where(RequestRow.request_name == request_name)
            .values(**kwargs)
            .returning(RequestRow)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one_or_none()

    async def get_non_terminal_requests(self) -> list[RequestRow]:
        terminal = ("completed", "failed", "aborted")
        stmt = select(RequestRow).where(RequestRow.status.notin_(terminal))
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def count_requests_by_status(self) -> dict[str, int]:
        stmt = select(RequestRow.status, func.count()).group_by(RequestRow.status)
        result = await self.session.execute(stmt)
        return dict(result.all())

    # ── Workflows ───────────────────────────────────────────────

    async def create_workflow(self, **kwargs: Any) -> WorkflowRow:
        row = WorkflowRow(**kwargs)
        self.session.add(row)
        await self.session.flush()
        return row

    async def get_workflow(self, workflow_id: UUID) -> WorkflowRow | None:
        result = await self.session.execute(
            select(WorkflowRow).where(WorkflowRow.id == workflow_id)
        )
        return result.scalar_one_or_none()

    async def get_workflow_by_request(self, request_name: str) -> WorkflowRow | None:
        result = await self.session.execute(
            select(WorkflowRow).where(WorkflowRow.request_name == request_name)
        )
        return result.scalar_one_or_none()

    async def update_workflow(self, workflow_id: UUID, **kwargs: Any) -> WorkflowRow | None:
        kwargs["updated_at"] = datetime.now(timezone.utc)
        stmt = (
            update(WorkflowRow)
            .where(WorkflowRow.id == workflow_id)
            .values(**kwargs)
            .returning(WorkflowRow)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one_or_none()

    async def get_queued_requests(self, limit: int = 1) -> list[RequestRow]:
        stmt = (
            select(RequestRow)
            .where(RequestRow.status == "queued")
            .order_by(RequestRow.priority.desc(), RequestRow.created_at.asc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    # ── DAGs ────────────────────────────────────────────────────

    async def create_dag(self, **kwargs: Any) -> DAGRow:
        row = DAGRow(**kwargs)
        self.session.add(row)
        await self.session.flush()
        return row

    async def get_dag(self, dag_id: UUID) -> DAGRow | None:
        result = await self.session.execute(
            select(DAGRow).where(DAGRow.id == dag_id)
        )
        return result.scalar_one_or_none()

    async def update_dag(self, dag_id: UUID, **kwargs: Any) -> DAGRow | None:
        stmt = (
            update(DAGRow)
            .where(DAGRow.id == dag_id)
            .values(**kwargs)
            .returning(DAGRow)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one_or_none()

    async def count_active_dags(self) -> int:
        stmt = select(func.count()).select_from(DAGRow).where(
            DAGRow.status.in_(("submitted", "running"))
        )
        result = await self.session.execute(stmt)
        return result.scalar_one()

    # ── DAG History ─────────────────────────────────────────────

    async def create_dag_history(self, **kwargs: Any) -> DAGHistoryRow:
        row = DAGHistoryRow(**kwargs)
        self.session.add(row)
        await self.session.flush()
        return row

    async def get_dag_history(self, dag_id: UUID) -> list[DAGHistoryRow]:
        stmt = (
            select(DAGHistoryRow)
            .where(DAGHistoryRow.dag_id == dag_id)
            .order_by(DAGHistoryRow.created_at.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    # ── Output Datasets ────────────────────────────────────────

    async def create_output_dataset(self, **kwargs: Any) -> OutputDatasetRow:
        row = OutputDatasetRow(**kwargs)
        self.session.add(row)
        await self.session.flush()
        return row

    async def get_output_datasets(self, workflow_id: UUID) -> list[OutputDatasetRow]:
        stmt = select(OutputDatasetRow).where(OutputDatasetRow.workflow_id == workflow_id)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def update_output_dataset(
        self, dataset_id: UUID, **kwargs: Any
    ) -> OutputDatasetRow | None:
        kwargs["updated_at"] = datetime.now(timezone.utc)
        stmt = (
            update(OutputDatasetRow)
            .where(OutputDatasetRow.id == dataset_id)
            .values(**kwargs)
            .returning(OutputDatasetRow)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one_or_none()

    # ── Sites ───────────────────────────────────────────────────

    async def upsert_site(self, **kwargs: Any) -> SiteRow:
        kwargs["updated_at"] = datetime.now(timezone.utc)
        stmt = pg_insert(SiteRow).values(**kwargs)
        update_cols = {k: v for k, v in kwargs.items() if k != "name"}
        stmt = stmt.on_conflict_do_update(index_elements=["name"], set_=update_cols)
        stmt = stmt.returning(SiteRow)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one()

    async def get_site(self, name: str) -> SiteRow | None:
        result = await self.session.execute(
            select(SiteRow).where(SiteRow.name == name)
        )
        return result.scalar_one_or_none()

    async def list_sites(self) -> list[SiteRow]:
        result = await self.session.execute(select(SiteRow).order_by(SiteRow.name))
        return list(result.scalars().all())
