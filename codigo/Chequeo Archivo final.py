import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def validate_and_report(file_path: str, output_report: str) -> None:
    """
    Realiza un chequeo general del archivo CSV y genera un informe para identificar posibles problemas.

    Args:
        file_path (str): Ruta del archivo limpio.
        output_report (str): Ruta donde se guardará el informe.
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Archivo cargado para validación: {file_path}")
    except Exception as e:
        logging.error(f"Error al cargar el archivo: {e}")
        return

    with open(output_report, 'w') as report:
        # 1. Resumen general
        report.write("=== Resumen General ===\n")
        report.write(f"Dimensiones: {df.shape[0]} filas, {df.shape[1]} columnas.\n\n")
        report.write("Columnas del archivo:\n")
        report.write(", ".join(df.columns) + "\n\n")

        # 2. Valores nulos
        report.write("=== Valores Nulos ===\n")
        null_percentage = df.isnull().mean() * 100
        null_columns = null_percentage[null_percentage > 0]
        if null_columns.empty:
            report.write("No se encontraron valores nulos.\n\n")
        else:
            report.write("Porcentaje de nulos por columna:\n")
            report.write(null_columns.to_string() + "\n\n")

        # 3. Valores duplicados en columnas críticas
        report.write("=== Valores Duplicados ===\n")
        if 'EVENT_ID' in df.columns:
            duplicates = df.duplicated(subset=['EVENT_ID']).sum()
            report.write(f"Duplicados en 'EVENT_ID': {duplicates}\n")

            # Análisis de duplicados parciales
            report.write("=== Análisis de Duplicados Parciales ===\n")
            duplicados_parciales = df[df.duplicated(subset=['EVENT_ID'], keep=False)]
            if not duplicados_parciales.empty:
                report.write(f"Duplicados parciales detectados: {len(duplicados_parciales)} registros.\n")
                # Identificar columnas con diferencias
                columnas_con_diferencias = []
                for col in duplicados_parciales.columns:
                    if duplicados_parciales.groupby('EVENT_ID')[col].nunique().max() > 1:
                        columnas_con_diferencias.append(col)
                report.write(f"Columnas con diferencias entre duplicados: {columnas_con_diferencias}\n")
                # Mostrar un ejemplo representativo
                ejemplo_duplicados = duplicados_parciales.groupby('EVENT_ID').filter(lambda x: len(x) > 1).head(10)
                report.write("Ejemplo de duplicados parciales:\n")
                report.write(ejemplo_duplicados.to_string(index=False) + "\n\n")
            else:
                report.write("No se detectaron duplicados parciales.\n\n")
        else:
            report.write("Columna 'EVENT_ID' no encontrada.\n")
        report.write("\n")

        # 4. Validación de valores fuera de rango
        report.write("=== Valores Fuera de Rango ===\n")
        if 'DAMAGE_CROPS' in df.columns:
            out_of_range_crops = df[df['DAMAGE_CROPS'] < 0]
            report.write(f"Registros con 'DAMAGE_CROPS' negativos: {len(out_of_range_crops)}\n")
        if 'BEGIN_LAT' in df.columns and 'BEGIN_LON' in df.columns:
            invalid_lat = df[(df['BEGIN_LAT'] < -90) | (df['BEGIN_LAT'] > 90)]
            invalid_lon = df[(df['BEGIN_LON'] < -180) | (df['BEGIN_LON'] > 180)]
            report.write(f"Coordenadas inválidas: {len(invalid_lat)} latitudes y {len(invalid_lon)} longitudes.\n")
        report.write("\n")

        # 5. Estadísticas
        report.write("=== Estadísticas ===\n")
        numerical_cols = df.select_dtypes(include=['number']).columns
        if not numerical_cols.empty:
            stats = df[numerical_cols].describe()
            report.write(stats.to_string() + "\n\n")
        else:
            report.write("No se encontraron columnas numéricas.\n\n")

        report.write("=== Informe Generado ===\n")
        report.write("El archivo contiene problemas.\n")

    logging.info(f"Informe de validación generado: {output_report}")

if __name__ == "__main__":
    # Ruta del archivo a validar
    input_file = "Datos NOAA 2024 limpios.csv"
    # Ruta del informe generado
    output_file = "informe CSV final.txt"
    validate_and_report(input_file, output_file)
