from typing import List

from pydantic import BaseModel
from typing_extensions import Optional


class Pharmacy(BaseModel):
    chain: Optional[str]
    npi: Optional[str]

class Pharmacies(BaseModel):
    pharmacies: Optional[List[Pharmacy]]