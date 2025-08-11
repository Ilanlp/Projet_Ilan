from fastapi import APIRouter
from app.db.connection import get_connection

router = APIRouter(
    prefix="/candidat",
    tags=["Candidat"],
    dependencies=[]  # Auth peut être mis par endpoint
)

@router.get("/ville")
def get_users():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ville  FROM DIM_LIEU ")
        result = cursor.fetchall()
        return {"data": result}
    finally:
        cursor.close()
        conn.close()


@router.get("/departement")
def get_users():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT departement  FROM DIM_LIEU GROUP BY departement")
        result = cursor.fetchall()
        return {"data": result}
    finally:
        cursor.close()
        conn.close()

@router.get("/region")
def get_users():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT region FROM DIM_LIEU GROUP BY region ")
        result = cursor.fetchall()
        return {"data": result}
    finally:
        cursor.close()
        conn.close()

@router.get("/contrat")
def get_users():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT type_contrat FROM DIM_CONTRAT")
        result = cursor.fetchall()
        return {"data": result}
    finally:
        cursor.close()
        conn.close()