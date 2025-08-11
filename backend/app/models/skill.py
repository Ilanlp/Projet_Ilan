from pydantic import BaseModel
from typing import Optional

class SkillCreate(BaseModel):
    skill: str
    type: Optional[str] = None

class Skill(SkillCreate):
    id_competence: int