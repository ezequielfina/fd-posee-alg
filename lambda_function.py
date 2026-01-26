import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname) - [%(funcName)s] - %(message)s',
    stream=sys.stdout,
    force=True
)

logger = logging.getLogger()


def get_db_connection():
    logger.info('Obteniendo conexión a DB.')

    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        connect_timeout=5,
        cursor_factory=RealDictCursor
    )


def update_load_status(conn, file_key, status):
    """Actualiza el estado. No capturamos error aquí para que explote en el main si falla."""
    logger.info(f"Actualizando estado a {status}")

    with conn.cursor() as cur:
        query = "UPDATE cargas SET status = %s WHERE nombre_archivo = %s"
        cur.execute(query, (status, file_key))
        conn.commit()


def read_current_status(conn, file_key) -> bool:
    logger.info("Leyendo estado previo.")

    with conn.cursor() as cur:
        query = "SELECT status FROM cargas WHERE nombre_archivo = %s"
        cur.execute(query, (file_key,))
        registro = cur.fetchone()

        logger.info(f"registro: {registro['status']}")

        return registro and registro['status'] == 'RAW'


def get_id_carga(conn, file_key: str) -> str:
    logger.info("Obteniendo id_carga.")

    with conn.cursor() as cur:
        query = "SELECT id FROM cargas WHERE nombre_archivo = %s"
        cur.execute(query, (file_key, ))

        id_carga = cur.fetchone()

        logger.info(f"id_carga {id_carga['id']}")
        return id_carga['id']


def get_arn_script(conn, file_key):
    logger.info("Intentando obtener scripts de validación y transformación.")

    with conn.cursor() as cur:
        # 1. Usamos SELECT * para que traiga columnas separadas: v_script y t_script
        query = "SELECT * FROM obtener_script_carga(%s)"
        cur.execute(query, (file_key,))

        # fetchone() nos dará una tupla (o dict si configuraste RealDictCursor)
        result = cur.fetchone()

    id_carga_uso = get_id_carga(conn, file_key)
    if not id_carga_uso:
        return {
            "status": "rejected",
            "file_key": f"raw/{file_key}"
        }

    # Si no hay resultado en la DB
    if not result:
        update_load_status(conn, file_key, "VALIDATED - WITHOUT ANY SCRIPT")
        return {
            "status": "success",
            "file_key": f"raw/{file_key}",
            "id_carga": id_carga_uso
        }

    script_val, script_tra = result['v_script'], result['t_script']

    # Tu lógica de negocio
    if script_val and script_tra:
        update_load_status(conn, file_key, "VALIDATED - WITH SCRIPT VAL AND TRA")
        return {
            "status": "success",
            "script_val": script_val,
            "script_tra": script_tra,
            "file_key": f"raw/{file_key}",
            "id_carga": id_carga_uso
        }
    elif script_val:
        update_load_status(conn, file_key, "VALIDATED - WITH SCRIPT VAL NOT TRA")
        return {
            "status": "success",
            "script_val": script_val,
            "file_key": f"raw/{file_key}",
            "id_carga": id_carga_uso
        }
    else:
        update_load_status(conn, file_key, "VALIDATED - WITHOUT ANY SCRIPT")
        return {
            "status": "success",
            "file_key": f"raw/{file_key}",
            "id_carga": id_carga_uso
        }


# --- HANDLER PRINCIPAL (Único lugar con Try/Except complejo) ---

def lambda_handler(event, context):
    logger.info("INICIO del lambda function.")

    # Accedemos directo al bloque 'detail'
    full_path = event['detail']['object']['key']
    file_key_db = full_path.replace('raw/', '', 1)

    logger.info(f"file_key {file_key_db}")

    conn = None

    try:
        conn = get_db_connection()

        # 1. Validar estado previo
        if not read_current_status(conn, file_key_db):
            return {
                "status": "rejected",
                "reason": "El estado previo no es RAW o no existe el registro",
                "file_key": file_key_db
            }

        # 2. Transición y búsqueda de script
        update_load_status(conn, file_key_db, 'VERIFICANDO SI POSEE ALGORITMO')
        return get_arn_script(conn, file_key_db)

    except Exception as e:
        error_msg = f"Fallo crítico: {str(e)}"
        logger.error(f"Error en la conexión con la DB: {error_msg}")

        return {"status": "error", "message": "Error en la conexión con la DB"}
    finally:
        if conn:
            conn.close()
