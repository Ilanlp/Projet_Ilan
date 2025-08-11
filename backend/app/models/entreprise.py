from pydantic import BaseModel
from typing import Optional

class EntrepriseCreate(BaseModel):
    nom: str
    type_entreprise: Optional[str] = None

class Entreprise(EntrepriseCreate):
    id_entreprise: int