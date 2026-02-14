from pydantic import BaseModel, Field


class StatusTransition(BaseModel):
    model_config = {"populate_by_name": True}

    from_status: str = Field(alias="from")
    to_status: str = Field(alias="to")
    timestamp: str
