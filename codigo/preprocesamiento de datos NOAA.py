
import pandas as pd
import logging
import re
import unicodedata
import tkinter as tk
from tkinter import filedialog, messagebox

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- Funciones Auxiliares Generales ---

def cargar_datos(ruta_archivo: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(ruta_archivo)
        logging.info(f"Archivo cargado: {ruta_archivo}")
        return df
    except Exception as e:
        logging.error(f"Error al cargar el archivo: {e}")
        raise

def convertir_damage(value: str) -> float:
    if not isinstance(value, str):
        return value if pd.notnull(value) else 0
    value = value.upper().strip()
    match = re.match(r"(\d+(\.\d+)?)([KMB])?", value)
    if match:
        number = float(match.group(1))
        multiplier = {"K": 1e3, "M": 1e6, "B": 1e9}.get(match.group(3), 1)
        return number * multiplier
    return 0

def normalizar_nombre_columna(col: str) -> str:
    col = col.strip().lower().replace(" ", "_")
    col = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode("utf-8")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col

def preprocesamiento_general(df: pd.DataFrame, umbral_nulos: float = 0.9) -> pd.DataFrame:
    logging.info("Iniciando preprocesamiento general del archivo...")

    # renombra columnas
    df.columns = [normalizar_nombre_columna(c) for c in df.columns]

    # elimina columnas completamente vacías
    df = df.dropna(axis=1, how='all')

    # elimina columnas con demasiados nulos
    porcentaje_nulos = df.isnull().mean()
    columnas_a_eliminar = porcentaje_nulos[porcentaje_nulos > umbral_nulos].index
    df = df.drop(columns=columnas_a_eliminar)

    # reemplazo de valores vacíos
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(["", "nan", "NaN"], pd.NA)

    # detección de fechas
    for col in df.columns:
        if 'date' in col or 'fecha' in col or 'time' in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if df[col].isnull().mean() > 0.5:
                    logging.warning(f"La columna '{col}' tiene muchas fechas inválidas o mezcla de formatos.")
            except Exception as e:
                logging.warning(f"No se pudo convertir a fecha la columna '{col}': {e}")

    # optimización numericos
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')

    # elimina duplicados
    df = df.drop_duplicates()

    logging.info("Preprocesamiento general completo.")
    return df

def generar_reporte(df: pd.DataFrame, output_path: str):
    with open(output_path, 'w') as report:
        report.write("=== Resumen General ===\n")
        report.write(f"Dimensiones: {df.shape[0]} filas, {df.shape[1]} columnas.\n\n")
        report.write("Columnas:\n")
        report.write(", ".join(df.columns) + "\n\n")
        report.write("=== Valores Nulos por Columna ===\n")
        report.write(df.isnull().sum().to_string() + "\n\n")
        report.write("=== Cardinalidad de Columnas ===\n")
        report.write(df.nunique().to_string())

def detectar_tipo_archivo(df: pd.DataFrame) -> str:
    cols = df.columns
    if 'damage_property' in cols:
        return 'details'
    elif 'fatality_type' in cols:
        return 'fatalities'
    elif 'begin_lat' in cols and 'begin_location' in cols:
        return 'locations'
    else:
        return 'desconocido'

# --- Interfaz gráfica ---

def seleccionar_archivo() -> str:
    root = tk.Tk()
    root.withdraw()
    ruta_archivo = filedialog.askopenfilename(title="Selecciona archivo CSV", filetypes=[("CSV files", "*.csv")])
    return ruta_archivo

def guardar_archivo(df: pd.DataFrame):
    ruta_guardado = filedialog.asksaveasfilename(title="Guardar archivo procesado", defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    if ruta_guardado:
        df.to_csv(ruta_guardado, index=False)
        logging.info(f"Archivo guardado: {ruta_guardado}")

# --- Ejecución Principal ---

def main():
    archivo = seleccionar_archivo()
    if not archivo:
        logging.error("No se seleccionó ningún archivo.")
        return

    df = cargar_datos(archivo)
    df = preprocesamiento_general(df)

    tipo = detectar_tipo_archivo(df)
    logging.info(f"Tipo de archivo detectado: {tipo}")

    if tipo == 'details':
        df['damage_property'] = df['damage_property'].apply(convertir_damage)

    generar_reporte(df, "reporte_validacion.txt")
    guardar_archivo(df)
    messagebox.showinfo("Proceso terminado", "Los datos han sido preprocesados y guardados exitosamente.")

if __name__ == "__main__":
    main()
