from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class Claim(BaseModel):
    id: Optional[str]
    ndc: Optional[int]
    npi: Optional[int]
    quantity: Optional[float] = 0
    price: Optional[float]
    timestamp: Optional[datetime]


class Claims(BaseModel):
    claims: Optional[List[Claim]]
