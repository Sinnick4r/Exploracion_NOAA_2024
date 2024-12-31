import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def cargar_datos(ruta_archivo: str) -> pd.DataFrame:
    """
    Carga un archivo CSV en un DataFrame.

    Args:
        ruta_archivo (str): Ruta del archivo CSV.

    Returns:
        pd.DataFrame: DataFrame cargado.
    """
    try:
        df = pd.read_csv(ruta_archivo)
        logging.info(f"Archivo cargado: {ruta_archivo}")
        return df
    except Exception as e:
        logging.error(f"Error al cargar el archivo: {e}")
        raise

def manejar_columnas_con_nulos(df: pd.DataFrame, umbral: float = 0.9) -> pd.DataFrame:
    """
    Maneja columnas con un alto porcentaje de nulos.
    - Elimina columnas con más del 90% de nulos.

    Args:
        df (pd.DataFrame): DataFrame original.
        umbral (float): Porcentaje de nulos permitido antes de eliminar una columna.

    Returns:
        pd.DataFrame: DataFrame con columnas relevantes.
    """
    porcentaje_nulos = df.isnull().mean()
    columnas_a_eliminar = porcentaje_nulos[porcentaje_nulos > umbral].index.tolist()
    if columnas_a_eliminar:
        logging.info(f"Columnas eliminadas oir mayoria de nulos: {columnas_a_eliminar}")
        df = df.drop(columns=columnas_a_eliminar)
    else:
        logging.info("No se encontraron columnas con mayoria de nulos.")
    return df

def marcar_duplicados_event_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Segun el informe de limpieza, EVENT_ID tiene duplicados parciales porque cada uno tiene diferentes FATALITY_ID (es decir, diferentes fatalidades)
    Crea una columna indicando si un EVENT_ID está duplicado, para asi tener registro de cuantos eventos unicos hay para posterior visualizacion.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame con una columna adicional 'ES_EVENT_ID_DUPLICADO'.
    """
    if 'EVENT_ID' in df.columns:
        df['ES_EVENT_ID_DUPLICADO'] = df.duplicated(subset=['EVENT_ID'], keep=False)
        logging.info("Columna 'ES_EVENT_ID_DUPLICADO' creada para identificar duplicados de EVENT_ID.")
    else:
        logging.warning("La columna 'EVENT_ID' no se encuentra en el DataFrame.")
    return df

def manejar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Maneja valores nulos en el DF.
    - Reemplaza nulos en columnas numéricas con 0.
    - Reemplaza nulos en columnas categóricas con 'Desconocido'.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame con valores nulos manejados.
    """
    columnas_con_nulos = df.columns[df.isnull().any()].tolist()
    for col in columnas_con_nulos:
        if df[col].dtype in ['float64', 'int64']:
            df[col] = df[col].fillna(0)
            logging.info(f"Valores nulos en la columna '{col}' reemplazados con 0.")
        else:
            df[col] = df[col].fillna("Desconocido")
            logging.info(f"Valores nulos en la columna '{col}' reemplazados con 'Desconocido'.")
    return df

def validar_consistencia(df: pd.DataFrame) -> None:
    """
    Valida la consistencia general del DF:
    - Comprueba relaciones entre columnas, como fechas de inicio y fin.
    - Revisa valores fuera de rango.

    Args:
        df (pd.DataFrame): DataFrame a validar.
    """
    # Validación de fechas
    if 'BEGIN_DATE' in df.columns and 'END_DATE' in df.columns:
        fechas_inconsistentes = df[df['BEGIN_DATE'] > df['END_DATE']]
        logging.info(f"Registros con fechas inconsistentes: {len(fechas_inconsistentes)}")
    else:
        logging.warning("No se encontraron columnas 'BEGIN_DATE' y 'END_DATE' para validar fechas.")

    # Validación de valores fuera de rango
    if 'BEGIN_LAT' in df.columns and 'BEGIN_LON' in df.columns:
        lat_fuera_rango = df[(df['BEGIN_LAT'] < -90) | (df['BEGIN_LAT'] > 90)]
        lon_fuera_rango = df[(df['BEGIN_LON'] < -180) | (df['BEGIN_LON'] > 180)]
        logging.info(f"Registros con latitudes fuera de rango: {len(lat_fuera_rango)}")
        logging.info(f"Registros con longitudes fuera de rango: {len(lon_fuera_rango)}")

def guardar_datos(df: pd.DataFrame, ruta_salida: str) -> None:
    """
    Guarda el DF limpio en un archivo CSV.

    Args:
        df (pd.DataFrame): DataFrame a guardar.
        ruta_salida (str): Ruta del archivo de salida.
    """
    try:
        df.to_csv(ruta_salida, index=False)
        logging.info(f"Archivo limpio guardado en: {ruta_salida}")
    except Exception as e:
        logging.error(f"Error al guardar el archivo: {e}")
        raise

def main(ruta_entrada: str, ruta_salida: str) -> None:
    """
    Ejecuta el proceso de limpieza:
    - Carga de datos.
    - Eliminación de columnas con mayoría de nulos.
    - Marcado de duplicados.
    - Manejo de nulos.
    - Validación de consistencia.
    - Guardado de datos limpios.

    Args:
        ruta_entrada (str): Ruta del archivo original.
        ruta_salida (str): Ruta del archivo limpio final.
    """
    # Cargar datos
    df = cargar_datos(ruta_entrada)

    # Manejar columnas con mayoría de nulos
    df = manejar_columnas_con_nulos(df)

    # Marcar duplicados
    df = marcar_duplicados_event_id(df)

    # Manejar nulos
    df = manejar_nulos(df)

    # Validar consistencia
    validar_consistencia(df)

    # Guardar datos limpios
    guardar_datos(df, ruta_salida)

if __name__ == "__main__":
    # Ruta del archivo original
    ruta_entrada = "Datos NOAA 2024 limpios.csv"
    # Ruta del archivo limpio final
    ruta_salida = "Datos_NOAA_2024_limpios_final.csv"
    main(ruta_entrada, ruta_salida)
