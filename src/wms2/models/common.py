from pydantic import BaseModel


class StatusTransition(BaseModel):
    from_status: str
    to_status: str
    timestamp: str
