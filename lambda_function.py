import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_db_connection():
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
    with conn.cursor() as cur:
        query = "UPDATE cargas SET status = %s WHERE nombre_archivo = %s"
        cur.execute(query, (status, file_key))
        conn.commit()
        logger.info(f"Estado de {file_key} actualizado a {status}")


def read_current_status(conn, file_key) -> bool:
    with conn.cursor() as cur:
        query = "SELECT status FROM cargas WHERE nombre_archivo = %s"
        cur.execute(query, (file_key,))
        registro = cur.fetchone()
        return registro and registro['status'] == 'RAW'


def get_arn_script(conn, file_key):
    """Retorna el ARN o None. Si falla, el error sube al handler."""
    with conn.cursor() as cur:
        query = "SELECT obtener_script_carga(%s) as script_name"
        cur.execute(query, (file_key,))
        result = cur.fetchone()

        script = result['script_name'] if result else None

        if script:
            update_load_status(conn, file_key, "VALIDATED - WITH SCRIPT")
            return {"status": "success", "script": script}
        else:
            update_load_status(conn, file_key, "VALIDATED - WITHOUT SCRIPT")
            return {"status": "success"}


# --- HANDLER PRINCIPAL (Único lugar con Try/Except complejo) ---

def lambda_handler(event, context):
    # Accedemos directo al bloque 'detail'
    file_key = event['detail']['object']['key']
    conn = None

    try:
        conn = get_db_connection()

        # 1. Validar estado previo
        if not read_current_status(conn, file_key):
            update_load_status(conn, file_key, 'REJECTED')
            return {"status": "rejected", "reason": "El estado previo no es RAW o no existe el registro"}

        # 2. Transición y búsqueda de script
        update_load_status(conn, file_key, 'VERIFICANDO SI POSEE ALGORITMO')
        return get_arn_script(conn, file_key)

    except Exception as e:
        error_msg = f"Fallo crítico: {str(e)}"
        logger.error(error_msg)
        if conn:
            try:
                # Intentamos marcar el error en la DB
                update_load_status(conn, file_key, 'FAILED')
            except Exception:  # <-- Cambiado de bare except a Exception
                # Si llegamos aquí es porque la conexión a la DB se rompió físicamente
                logger.warning("No se pudo actualizar el estado a FAILED porque la DB no responde.")
        return {"status": "error", "message": error_msg}
    finally:
        if conn:
            conn.close()
