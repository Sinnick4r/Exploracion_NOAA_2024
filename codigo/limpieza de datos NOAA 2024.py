import pandas as pd
import re
import tkinter as tk
from tkinter import filedialog, messagebox
import io
import logging

# ----------------------------------------------------------------------
# Configuración básica de logging
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ----------------------------------------------------------------------
# Funciones auxiliares
# ----------------------------------------------------------------------

def convertir_damage(value: str) -> float:
    """
    Convierte la columna DAMAGE_PROPERTY a un valor numérico.
    Se fija en los sufijos 'K' (miles), 'M' (millones) y 'B' (billones).
    En caso de no poder , devuelve 0.
    """

    if not isinstance(value, str):
        return value if pd.notnull(value) else 0

    value = value.upper().strip()
    match = re.match(r"(\d+(\.\d+)?)([KMB])?", value)
    if match:
        number_str = match.group(1)
        suffix = match.group(3)

        try:
            number = float(number_str)
        except ValueError:
            return 0

        if suffix == 'K':
            return number * 1_000
        elif suffix == 'M':
            return number * 1_000_000
        elif suffix == 'B':
            return number * 1_000_000_000
        else:
            return number
    else:
        try:
            return float(value)
        except ValueError:
            return 0

def selecciona_archivo(file_type: str) -> str:
    """
    Abre ventana para seleccionar un archivo CSV
  
    """
    file_path = filedialog.askopenfilename(
        title=f"Seleccionar archivo de {file_type}",
        filetypes=[("CSV files", "*.csv")]
    )
    return file_path

def cargar_csv(file_path: str, file_description: str) -> pd.DataFrame:
    """
    Carga un archivo CSV a un DF de pandas.

    """
    if not file_path:
        messagebox.showerror("Error", f"No se seleccionó el archivo de {file_description}.")
        return pd.DataFrame()

    try:
        df = pd.read_csv(file_path)
        messagebox.showinfo("Archivo seleccionado", f"Archivo de {file_description} seleccionado: {file_path}")
        logging.info(f"{file_description.capitalize()} cargado correctamente: {file_path}")
        return df
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo leer el archivo de {file_description}: {e}")
        logging.error(f"Error leyendo {file_description}: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------------
# Funciones de limpieza
# ----------------------------------------------------------------------

def manejar_nulos(df: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
    """
    Elimina columnas con un porcentaje de nulos mayor al umbral especificado.
    """
    null_percentage = df.isnull().mean()
    columns_to_drop = null_percentage[null_percentage > threshold].index
    logging.info(f"Eliminando columnas con más del {threshold*100}% de valores nulos: {list(columns_to_drop)}")
    df.drop(columns=columns_to_drop, inplace=True)
    return df

def fuera_de_rango(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige valores fuera de rango en columnas específicas.

    """
    if 'DAMAGE_PROPERTY' in df.columns:
        df['DAMAGE_PROPERTY'] = df['DAMAGE_PROPERTY'].apply(lambda x: x if x >= 0 else 0)
    if 'DAMAGE_CROPS' in df.columns:
        df['DAMAGE_CRP¨S'] = df['DAMAGE_CROPS'].apply(lambda x: x if x >= 0 else 0)
    logging.info("Valores fuera de rango corregidos.")
    return df

def convertir_tiempo(df: pd.DataFrame, year_month_col: str = None, day_col: str = None, time_col: str = None, output_col_prefix: str = None, date_format: str = '%Y%m', output_format: str = None) -> pd.DataFrame:
    """
    Convierte columnas de año/mes, día y/o hora a un formato datetime.

    Args:
        df (pd.DataFrame): DataFrame que contiene las columnas a convertir.
        year_month_col (str, opcional): Nombre de la columna con el año y mes.
        day_col (str, opcional): Nombre de la columna con el día.
        time_col (str, opcional): Nombre de la columna con la hora en formato HHMM.
        output_col_prefix (str, opcional): Prefijo para las columnas de salida.
        date_format (str): Formato de la columna year_month_col.
        output_format (str, opcional): Formato deseado para la salida. Si no se tiene, se mantiene como datetime.

    Returns:
        pd.DataFrame: DataFrame con las columnas convertidas.
    """
    try:
        if output_col_prefix is None:
            output_col_prefix = "RESULT"

        if year_month_col and day_col:
            if year_month_col in df.columns and day_col in df.columns:
                df[f'{output_col_prefix}_DATE'] = pd.to_datetime(
                    df[year_month_col].astype(str) + df[day_col].astype(str).str.zfill(2),
                    format=date_format + '%d',
                    errors='coerce'
                )
                if output_format:
                    df[f'{output_col_prefix}_DATE'] = df[f'{output_col_prefix}_DATE'].dt.strftime(output_format)
                logging.info(f"Columnas '{year_month_col}' y '{day_col}' combinadas y convertidas exitosamente a datetime.")

        if time_col:
            if time_col in df.columns:
                df[f'{output_col_prefix}_TIME'] = pd.to_datetime(
                    df[time_col].astype(str).str.zfill(4),
                    format='%H%M',
                    errors='coerce'
                ).dt.time
                logging.info(f"Columna '{time_col}' convertida exitosamente a formato de hora.")
    except Exception as e:
        logging.error(f"Error al convertir las columnas: {e}")
    return df

def limpiar_detalles(detalles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza específica para el DataFrame de detalles.
    """
    detalles_df['DAMAGE_PROPERTY'] = detalles_df['DAMAGE_PROPERTY'].fillna('0').apply(convertir_damage)
    detalles_df['DAMAGE_CROPS'] = detalles_df['DAMAGE_CROPS'].fillna('0').apply(convertir_damage)
    columns_to_drop = ['STATE_FIPS', 'CZ_FIPS', 'DATA_SOURCE', 'WFO', 'CATEGORY']
    detalles_df.drop(columns=[col for col in columns_to_drop if col in detalles_df.columns], inplace=True)
    return detalles_df

def limpiar_fatalidades(fatalidades_df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza específica para el DataFrame de fatalidades.

    """
    fatalidades_df['FATALITY_AGE'] = fatalidades_df['FATALITY_AGE'].fillna(-1)
    fatalidades_df['FATALITY_SEX'] = fatalidades_df['FATALITY_SEX'].fillna('Desconocido')
    if 'FATALITY_LOCATION' in fatalidades_df.columns:
        fatalidades_df['FATALITY_LOCATION'] = fatalidades_df['FATALITY_LOCATION'].astype(str).str.strip().str.lower()
    return fatalidades_df

def limpiar_lugares(lugares_df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza específica para el DataFrame de lugares.
    """
    lugares_df.drop(columns=['LAT2', 'LON2'], errors='ignore', inplace=True)
    if 'EVENT_ID' in lugares_df.columns:
        lugares_df.drop_duplicates(subset=['EVENT_ID'], inplace=True)
    return lugares_df

def informe(df: pd.DataFrame) -> None:
    """
    Genera informe

    """
    logging.info("Generando informe...")
    nulls = df.isnull().mean() * 100
    logging.info(f"Porcentaje de valores nulos:\n{nulls}")
    num_cols = df.select_dtypes(include=['number']).columns
    for col in num_cols:
        logging.info(f"Rango de valores en '{col}': {df[col].min()} - {df[col].max()}")
    cat_cols = df.select_dtypes(include=['object']).columns
    for col in cat_cols:
        logging.info(f"'{col}' tiene {df[col].nunique()} valores únicos.")

# ----------------------------------------------------------------------
# Función principal
# ----------------------------------------------------------------------

def main():
    
    root = tk.Tk()
    root.withdraw()

    detalles_path = selecciona_archivo("detalles")
    fatalidades_path = selecciona_archivo("fatalidades")
    lugares_path = selecciona_archivo("lugares")

    detalles_df = cargar_csv(detalles_path, "detalles")
    fatalidades_df = cargar_csv(fatalidades_path, "fatalidades")
    lugares_df = cargar_csv(lugares_path, "lugares")

    if detalles_df.empty or fatalidades_df.empty or lugares_df.empty:
        messagebox.showwarning("Proceso terminado", "No se pudo continuar con la limpieza de datos.")
        root.destroy()
        raise SystemExit

    detalles_df = limpiar_detalles(detalles_df)
    fatalidades_df = limpiar_fatalidades(fatalidades_df)
    lugares_df = limpiar_lugares(lugares_df)

    detalles_df = manejar_nulos(detalles_df)
    detalles_df = fuera_de_rango(detalles_df)

    master_df = detalles_df.merge(fatalidades_df, on='EVENT_ID', how='left').merge(lugares_df, on='EVENT_ID', how='left')

    master_df = convertir_tiempo(master_df, year_month_col='BEGIN_YEARMONTH', day_col='BEGIN_DAY', time_col='BEGIN_TIME', output_col_prefix='BEGIN')
    master_df = convertir_tiempo(master_df, year_month_col='END_YEARMONTH', day_col='END_DAY', time_col='END_TIME', output_col_prefix='END')

    informe(master_df)

    output_file = 'Datos NOAA 2024 limpios.csv'
    master_df.to_csv(output_file, index=False)
    messagebox.showinfo("Archivo Exportado", f"El archivo '{output_file}' ha sido creado correctamente.")
    logging.info(f"Archivo '{output_file}' exportado con éxito.")

    root.destroy()

# ----------------------------------------------------------------------
# Inicio
# ----------------------------------------------------------------------
if __name__ == "__main__":
    main()
