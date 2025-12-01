import polars as pl
import pandas as pd
import os
import re
import shutil
from pathlib import Path
from typing import List, Optional
from sqlalchemy import create_engine
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
#from tkinter import messagebox as dialogos # Se mantiene solo para la función de carga
import io
import pyodbc # Necesario para la conexión en el servidor

import streamlit as st  

# ==============================================================================
# 🎯 0. CONFIGURACIÓN INICIAL Y PARÁMETROS
# ==============================================================================

# --- PARÁMETROS DEL SERVIDOR SQL ---
# ¡AJUSTA ESTOS VALORES!
DB_SERVER = '192.168.50.53,1433' 
DB_NAME = 'CuotasIEP'
DB_TABLE = 'Cuotas_1_6IEP'
DB_USER = 'FinancieroConsultor'
DB_PASSWORD = 'IEP2025.'
SQL_DRIVER = "ODBC Driver 17 for SQL Server" # ¡Confirma que es el correcto!

# --- PARÁMETROS ETL ---
HOJAS_A_EXTRAER = ['MuestraActual']
ID_INDICES = [0, 1, 9]      
VALUE_INDICES = [2, 3, 4, 5, 6, 7, 8]
CSV_FILE_NAME = 'Consolidacion_Final_ETL_v2' # Ya no se usa para la carga a SQL

# --- CONEXIÓN DE BASE DE DATOS (SQLAlchemy) ---
CONNECTION_STRING = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
    f"?driver={SQL_DRIVER.replace(' ', '+')}"
)

# --- CONFIGURACIÓN DE FASTAPI ---
app = FastAPI(title="ETL Polars/SQL Server API")
# La carpeta 'templates' debe estar en el mismo nivel que este script
templates = Jinja2Templates(directory="templates")

# ==============================================================================
# 📥 1. FASE E: EXTRACCIÓN (Polars con Calamine)
# ==============================================================================

def extract_excel_data_optimizado(
    file_content: bytes, # Recibe el contenido binario del archivo subido
    sheet_names: List[str]
) -> pl.DataFrame:
    
    datos = []
    
    # Usamos io.BytesIO para que pandas pueda leer el archivo directamente de la memoria.
    excel_file_stream = io.BytesIO(file_content)
    
    for hoja in sheet_names:
        try:
            # Leer hoja con pandas (el motor 'openpyxl' es esencial)
            df_pd = pd.read_excel(excel_file_stream, sheet_name=hoja, header=None, engine="openpyxl")
            df_pd = df_pd.astype(str)
            
            df = pl.from_pandas(df_pd)
            df = df.cast(pl.Utf8) # Convertir TODO a texto
            
            # Extraer B1 (posición [0, 1] en Excel, que es [0, 0] en df si header=None)
            valorb1 = df[0, 0] if df.height > 1 and df.width > 1 else None
            
            # Seleccionar primeras 32 columnas por posición
            df_subset = df[:, :32].with_columns(
                pl.lit(valorb1).alias("FechaAnalisis")
            )
            
            datos.append(df_subset)
        
        except Exception as e:
            # Levantamos una excepción HTTP si una hoja crítica falla
            raise HTTPException(
                status_code=400, 
                detail=f"Error al leer la hoja '{hoja}'. Detalle: {e}"
            )

    if datos:
        return pl.concat(datos)
    else:
        return pl.DataFrame()

# ==============================================================================
# 🧹 2. FASE C: LIMPIEZA (Polars)
# ==============================================================================

def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    # --- 1. SELECCIÓN y RENOMBRADO ---
    select_expressions = [
        pl.col(df.columns[0]).alias('Indice'),
        pl.col(df.columns[1]).alias('Convocatoria'),
        pl.col(df.columns[25]).alias('RP'),
        pl.col(df.columns[26]).alias('Cuota1'),
        pl.col(df.columns[27]).alias('Cuota2'),
        pl.col(df.columns[28]).alias('Cuota3'),
        pl.col(df.columns[29]).alias('Cuota4'),
        pl.col(df.columns[30]).alias('Cuota5'),
        pl.col(df.columns[31]).alias('Cuota6'),
        pl.col('FechaAnalisis')
    ]
    df_limpio = df.select(select_expressions)

    # --- 2. Limpieza de Nulos inicial ---
    cols_a_string = ['Convocatoria', 'RP', 'Cuota1', 'Cuota2', 'FechaAnalisis']
    cleaned_df = df_limpio.with_columns([pl.col(col).cast(pl.Utf8) for col in cols_a_string])
    cleaned_df = cleaned_df.drop_nulls()  
    
    # --- 3. FILTRADO INVERSO: Eliminar filas con valores NUMÉRICOS usando RegEx ---
    cols_a_filtrar_valor = ['RP', 'Cuota1', 'Cuota2']
    patron_es_numerico = r"^\s*[-+]?\s*(\d*\.?\d+|\d+\.?\d*)\s*$"
    
    condiciones_numericas = [pl.col(col).str.contains(patron_es_numerico) for col in cols_a_filtrar_valor]
    
    condicion_general_numerica = (
        condiciones_numericas[0] |
        condiciones_numericas[1] |
        condiciones_numericas[2]
    )
    
    # Quedarse solo con las filas que NO son números
    cleaned_df = cleaned_df.filter(~condicion_general_numerica)
    
    # Eliminación final de nulos
    cleaned_df = cleaned_df.drop_nulls()
    
    return cleaned_df

# ==============================================================================
# 🔄 3. FASE T: TRANSFORMACIÓN (Polars Melt/Unpivot)
# ==============================================================================

# 🔄 3. FASE T: TRANSFORMACIÓN (Polars Unpivot) - CORREGIDA
# 🔄 4. FASE T: TRANSFORMACIÓN (Polars Melt/Unpivot) - CORREGIDA Y ALINEADA A SQL
def transform_data_unpivot_by_index(
    df: pl.DataFrame, 
    id_col_indices: List[int], 
    value_col_indices: List[int]
) -> pl.DataFrame:

    df_cols = df.columns
    id_cols = [df_cols[i] for i in id_col_indices]
    value_cols = [df_cols[i] for i in value_col_indices]

    # Dinamizar (Melt / Unpivot)
    transformed_df = df.unpivot( # Usamos unpivot para eliminar el DeprecationWarning
        index=id_cols,
        on=value_cols,
        variable_name="Metrica_Columna",
        value_name="Valor"
    )

    # 🛑 RENOMBRAMIENTO CLAVE: Usar ID_Fija_1, ID_Fija_2, ID_Fija_3
    # Esto alinea los nombres de Polars con los de la tabla SQL Server.
    final_col_names = {
        id_cols[i]: f'ID_Fija_{i+1}' for i in range(len(id_cols))
    }
    
    transformed_df = transformed_df.rename(final_col_names)
    
    print(f"Filas después de la transformación (melt/unpivot): {len(transformed_df)}")
    # El DataFrame final ahora tiene: ID_Fija_1, ID_Fija_2, ID_Fija_3, Metrica_Columna, Valor
    return transformed_df

# ==============================================================================
# 💾 4. FASE L: CARGA (Pandas/SQLAlchemy a SQL Server)
# ==============================================================================

def load_data_to_sql_server(
    df: pl.DataFrame,
    connection_string: str,
    table_name: str
):
    try:
        engine = create_engine(connection_string)
        
        # 1. Convertir Polars a Pandas
        df_pd_to_load = df.to_pandas()
        
        # 2. Carga masiva
        df_pd_to_load.to_sql(table_name, engine, if_exists='append', index=False)
        
        print(f"✅ Carga a SQL Server completada. Filas: {len(df_pd_to_load)}")
        return {"status": "success", "rows_loaded": len(df_pd_to_load)}

    except Exception as e:
        # Esto capturará errores de conexión, driver o SQL
        raise HTTPException(status_code=500, detail=f"Error en la Carga a SQL Server: {e}")

# ==============================================================================
# 🌐 5. ENDPOINTS DE LA APLICACIÓN
# ==============================================================================

# 1. ENDPOINT PRINCIPAL: Sirve la página web (El Cliente)
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Accedido por el navegador. Devuelve el HTML para la subida de archivos."""
    return templates.TemplateResponse("index.html", {"request": request})

# 2. ENDPOINT DE PROCESAMIENTO: Recibe el archivo y ejecuta el ETL completo
@app.post("/upload_and_process")
async def upload_and_process(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(('.xlsx', '.xlsm', '.xls')):
        raise HTTPException(status_code=400, detail="Tipo de archivo no válido. Debe ser Excel.")

    try:
        # 1. Lectura del contenido binario del archivo
        file_content = await file.read()
        
        # 2. Extracción (E)
        raw_df = extract_excel_data_optimizado(
            file_content=file_content,
            sheet_names=HOJAS_A_EXTRAER
        )

        if raw_df.is_empty():
            return JSONResponse(status_code=200, content={"message": "No se extrajo ninguna fila válida.", "rows_loaded": 0})
            
        # 3. Limpieza (C)
        cleaned_df = clean_data(raw_df)
        
        # 4. Transformación (T)
        final_transformed_df = transform_data_unpivot_by_index(
            df=cleaned_df,
            id_col_indices=ID_INDICES,
            value_col_indices=VALUE_INDICES
        )
        
        final_transformed_df = final_transformed_df.with_columns(pl.lit('Web_API').alias('Origen'))
        
        # 5. Carga (L)
        load_result = load_data_to_sql_server(
            df=final_transformed_df,
            connection_string=CONNECTION_STRING,
            table_name=DB_TABLE
        )
        
        return JSONResponse(status_code=200, content={
            "message": "PROCESO ETL FINALIZADO con éxito.",
            "filas_cargadas": load_result['rows_loaded'],
            "tabla_destino": DB_TABLE,
            "servidor_sql": DB_SERVER
        })

    except HTTPException as e:
        # Propaga errores HTTP controlados (ej. error de lectura de hoja)
        raise e
    except Exception as e:
        # Captura cualquier otro error
        raise HTTPException(status_code=500, detail=f"Fallo crítico del proceso ETL: {e}")



st.title("Servidor Web/API iniciado")
st.write("Accede a http://<IP_del_Servidor>:8000")





# ==============================================================================
# 🚀 6. INICIO DEL SERVIDOR (Uvicorn)
# ==============================================================================
if __name__ == "__main__":
    import uvicorn


    st.title("Servidor Web/API iniciado")
    st.write("Accede a http://<IP_del_Servidor>:8000")



    filename = st.file_uploader('Pick a file',accept_multiple_files=True)
    st.selectbox(f"{filename}")

    # 1. Lectura del contenido binario del archivo
    file_content = filename
    
    # 2. Extracción (E)
    raw_df = extract_excel_data_optimizado(
        file_content=file_content,
        sheet_names=HOJAS_A_EXTRAER
    )

        
    # 3. Limpieza (C)
    cleaned_df = clean_data(raw_df)
    
    # 4. Transformación (T)
    final_transformed_df = transform_data_unpivot_by_index(
        df=cleaned_df,
        id_col_indices=ID_INDICES,
        value_col_indices=VALUE_INDICES
    )
    
    final_transformed_df = final_transformed_df.with_columns(pl.lit('Web_API').alias('Origen'))
    
    # 5. Carga (L)
    load_result = load_data_to_sql_server(
        df=final_transformed_df,
        connection_string=CONNECTION_STRING,
        table_name=DB_TABLE
    )
    
    #final message

    st.write("Proceso Completado")
