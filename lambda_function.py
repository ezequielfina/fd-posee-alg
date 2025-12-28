import os
import logging
import psycopg2
import awswrangler as wr
import pandas as pd

# Configuración de Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# --- SUBFUNCIONES DE APOYO ---

def get_db_connection():
    """Gestiona la conexión a RDS usando variables de entorno."""
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        connect_timeout=5
    )


def update_load_status(conn, file_key, status):
    """Actualiza el estado en la tabla 'cargas' en RDS."""
    try:
        with conn.cursor() as cur:
            # Asumimos que la tabla se llama 'cargas' y filtramos por el nombre del objeto
            query = "UPDATE cargas SET estado = %s WHERE nombre_objeto = %s"
            cur.execute(query, (status, file_key))
            conn.commit()
            logger.info(f"Estado de {file_key} actualizado a {status}")
    except Exception as e:
        logger.error(f"Error actualizando estado en RDS: {e}")
        conn.rollback()


def read_s3_file(bucket, key):
    """Detecta la extensión y lee el archivo usando awswrangler."""
    path = f"s3://{bucket}/{key}"
    if key.lower().endswith('.xlsx'):
        logger.info(f"Leyendo Excel: {path}")
        return wr.s3.read_excel(path)
    elif key.lower().endswith('.csv'):
        logger.info(f"Leyendo CSV: {path}")
        return wr.s3.read_csv(path)
    else:
        raise ValueError(f"Formato no soportado para el archivo: {key}")


def validate_structure(df):
    """Lógica de validación con Pandas (aquí pones tus reglas)."""
    # Ejemplo simple: verificar que no esté vacío
    if df.empty:
        return False, "El archivo está vacío"
    # Podés agregar: if 'cuit' not in df.columns...
    return True, "OK"


# --- HANDLER PRINCIPAL ---

def lambda_handler(event, context):
    # 1. Extraer datos del evento S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    logger.info(f"Procesando archivo: {file_key} del bucket: {bucket}")

    conn = None
    try:
        # 2. Conectar a RDS
        conn = get_db_connection()

        # 3. Cambiar estado a 'VERIFICANDO' (Opcional pero recomendado)
        update_load_status(conn, file_key, 'VERIFICANDO')

        # 4. Leer archivo de S3
        df = read_s3_file(bucket, file_key)

        # 5. Validar estructura
        is_valid, message = validate_structure(df)

        # 6. Finalizar según validación
        if is_valid:
            update_load_status(conn, file_key, 'VALIDATED')
            return {"status": "success", "message": "Archivo validado correctamente"}
        else:
            update_load_status(conn, file_key, 'REJECTED')
            return {"status": "rejected", "reason": message}

    except Exception as e:
        logger.error(f"Fallo crítico en el proceso: {str(e)}")
        if conn:
            update_load_status(conn, file_key, 'FAILED')
        return {"status": "error", "message": str(e)}

    finally:
        if conn:
            conn.close()
            logger.info("Conexión a RDS cerrada.")