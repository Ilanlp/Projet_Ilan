from fastapi import APIRouter, HTTPException, Depends
from app.models.skill import Skill, SkillCreate
from app.core.security import verify_token
from app.db import queries
from typing import List

router = APIRouter(
    prefix="/skills",
    tags=["skills"],
    dependencies=[]  # Auth peut être mis par endpoint
)

@router.post("/", response_model=dict, dependencies=[Depends(verify_token)])
def create_skill(skill: SkillCreate):
    queries.create_skill(skill)
    return {"message": "Skill created successfully"}

@router.get("/", response_model=List[Skill])
def read_skills():
    return queries.get_skills()

@router.put("/{skill_id}", response_model=dict, dependencies=[Depends(verify_token)])
def update_skill(skill_id: str, skill: Skill):
    existing = queries.get_skill_by_id(skill_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Skill not found")
    queries.update_skill(skill_id, skill)
    return {"message": "Skill updated successfully"}

@router.delete("/{skill_id}", response_model=dict, dependencies=[Depends(verify_token)])
def delete_skill(skill_id: str):
    existing = queries.get_skill_by_id(skill_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Skill not found")
    queries.delete_skill(skill_id)
    return {"message": "Skill deleted successfully"}
