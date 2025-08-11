from pydantic import BaseModel
from typing import List

class SearchRequest(BaseModel):
    villes: List[str]
    departements: List[str]
    regions: List[str]
    skills: List[str]
