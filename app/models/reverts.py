from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class Revert(BaseModel):
    id: Optional[str]
    claim_id: Optional[str]
    timestamp: Optional[datetime]


class Reverts(BaseModel):
    reverts: Optional[List[Revert]]
