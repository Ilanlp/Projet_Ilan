from fastapi import APIRouter, HTTPException, Depends
from app.db.connection import get_connection
from app.models.entreprise import Entreprise, EntrepriseCreate
from app.core.security import verify_token

router = APIRouter(
    prefix="/entreprise",
    tags=["entreprise"],
    dependencies=[]  # Auth peut être mis par endpoint
)

@router.get("/")
def get_users():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM DIM_ENTREPRISE ")
        result = cursor.fetchall()
        return {"entreprise": result}
    finally:
        cursor.close()
        conn.close()

@router.post("/", response_model=dict, dependencies=[Depends(verify_token)])
def create_entreprise(entreprise: EntrepriseCreate):
    queries.create_entreprise(entreprise)
    return {"message": "Entreprise created successfully"}
