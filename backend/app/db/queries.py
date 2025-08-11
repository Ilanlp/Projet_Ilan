from app.db.connection import get_connection
from app.models.skill import Skill, SkillCreate
from typing import List, Optional

def create_skill(skill: SkillCreate):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dim_competence (skill, type)
            VALUES (%s, %s)
        """, (skill.skill, skill.type))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def get_skills() -> List[Skill]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id_competence, skill, type FROM dim_competence")
        rows = cursor.fetchall()
        return [Skill(id_competence=row[0], skill=row[1], type=row[2]) for row in rows]
    finally:
        cursor.close()
        conn.close()

def get_skill_by_id(skill_id: str) -> Optional[Skill]:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id_competence, skill, type FROM dim_competence WHERE id_competence = %s", (skill_id,))
        row = cursor.fetchone()
        if row:
            return Skill(id_competence=row[0], skill=row[1], type=row[2])
        return None
    finally:
        cursor.close()
        conn.close()

def update_skill(skill_id: str, skill: Skill):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE dim_competence
            SET skill = %s, type = %s
            WHERE id_competence = %s
        """, (skill.skill, skill.type, skill_id))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def delete_skill(skill_id: str):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dim_competence WHERE id_competence = %s", (skill_id,))
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def raw_query(query: str):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()