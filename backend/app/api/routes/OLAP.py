from fastapi import APIRouter
from app.db.connection import get_connection



router = APIRouter(
    tags=["OLAP"],
    dependencies=[]  # Auth peut être mis par endpoint
)

@router.get("/top_ville")
def get_olap_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                COUNT(*) AS "count", 
                l.ville AS "ville", 
                ROUND(AVG(l.latitude), 2) AS "latitude", 
                ROUND(AVG(l.longitude), 2) AS "longitude"
            FROM Fait_offre o 
            JOIN DIM_LIEU l ON o.id_lieu = l.id_lieu
            WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
            GROUP BY l.ville
            ORDER BY "count" DESC
            LIMIT 15;
        """
        
        cursor.execute(query)  # ⬅️ Tu avais oublié cette ligne

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


@router.get("/top_region")
def get_olap_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                COUNT(*) AS "count", 
                l.region AS "region", 
                ROUND(AVG(l.latitude), 2) AS "latitude", 
                ROUND(AVG(l.longitude), 2) AS "longitude"
            FROM Fait_offre o 
            JOIN DIM_LIEU l ON o.id_lieu = l.id_lieu
            WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
            GROUP BY l.region
            ORDER BY "count" DESC
            LIMIT 15;
        """
        
        cursor.execute(query)  # ⬅️ Tu avais oublié cette ligne

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass



@router.get("/top_departement")
def get_olap_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                COUNT(*) AS "count", 
                l.departement AS "departement", 
                ROUND(AVG(l.latitude), 2) AS "latitude", 
                ROUND(AVG(l.longitude), 2) AS "longitude"
            FROM Fait_offre o 
            JOIN DIM_LIEU l ON o.id_lieu = l.id_lieu
            WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
            GROUP BY l.departement
            ORDER BY "count" DESC
            LIMIT 15;
        """
        
        cursor.execute(query)  # ⬅️ Tu avais oublié cette ligne

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


@router.get("/top_skills")
def get_olap_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
         SELECT a.skill, COUNT(*) AS nb_offres
         FROM dim_competence a 
         JOIN LIAISON_OFFRE_COMPETENCE b on a.id_competence = b.id_competence 
         GROUP BY a.skill
         ORDER BY nb_offres DESC;
        """
        
        cursor.execute(query) 
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass