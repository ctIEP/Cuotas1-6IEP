import polars as pl
import pandas as pd
import uvicorn
import pyodbc
import os
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

# ==============================================================================
# 🎯 CONFIGURACIÓN (Usa variables de entorno para seguridad)
# ==============================================================================
# En el panel de Render pondrás estos valores
DB_SERVER = os.getenv("DB_SERVER", "192.168.50.53,1433")
DB_NAME = os.getenv("DB_NAME", "CuotasIEP")
DB_TABLE = os.getenv("DB_TABLE", "Cuotas_1_6IEP")
DB_USER = os.getenv("DB_USER", "FinancieroConsultor")
DB_PASS = os.getenv("DB_PASS", "IEP2025.")

# El driver en el Dockerfile de Linux se llama así:
DRIVER = "{ODBC Driver 17 for SQL Server}"

CONNECTION_STRING = (
    f"DRIVER={DRIVER};SERVER={DB_SERVER};DATABASE={DB_NAME};"
    f"UID={DB_USER};PWD={DB_PASS};Connection Timeout=30;"
)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- Funciones ETL (Mantenemos tu lógica de Polars) ---

def extract_excel_data(file_content, sheet_names: List[str]) -> pl.DataFrame:
    datos = []
    for hoja in sheet_names:
        try:
            df_pd = pd.read_excel(file_content, sheet_name=hoja, header=None, engine="openpyxl")
            df_pd = df_pd.astype(str)
            df = pl.from_pandas(df_pd).cast(pl.Utf8)
            valorb1 = df[0, 1] if df.height > 0 and df.width > 1 else "N/A"
            df_subset = df[:, :min(32, df.width)].with_columns(pl.lit(valorb1).alias("FechaAnalisis"))
            datos.append(df_subset)
        except: continue
    return pl.concat(datos, how="diagonal") if datos else pl.DataFrame()

def clean_and_transform(df: pl.DataFrame) -> pl.DataFrame:
    cols = df.columns
    df_limpio = df.select([
        pl.col(cols[0]).alias('ID_Fija_1'),
        pl.col(cols[1]).alias('ID_Fija_2'),
        pl.col(cols[25]).alias('RP'),
        pl.col(cols[26]).alias('Cuota1'),
        pl.col(cols[27]).alias('Cuota2'),
        pl.col(cols[28]).alias('Cuota3'),
        pl.col(cols[29]).alias('Cuota4'),
        pl.col(cols[30]).alias('Cuota5'),
        pl.col(cols[31]).alias('Cuota6'),
        pl.col('FechaAnalisis').alias('ID_Fija_3')
    ]).drop_nulls()
    
    patron = r"^\s*[-+]?\s*(\d*\.?\d+|\d+\.?\d*)\s*$"
    df_limpio = df_limpio.filter(~pl.col("RP").str.contains(patron))

    return df_limpio.unpivot(
        index=['ID_Fija_1', 'ID_Fija_2', 'ID_Fija_3'],
        on=['RP', 'Cuota1', 'Cuota2', 'Cuota3', 'Cuota4', 'Cuota5', 'Cuota6'],
        variable_name="Metrica_Columna", value_name="Valor"
    )

def load_to_sql(df: pl.DataFrame):
    df_pd = df.to_pandas()
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    columns = ", ".join([f"[{col}]" for col in df_pd.columns])
    placeholders = ", ".join(["?" for _ in df_pd.columns])
    query = f"INSERT INTO {DB_TABLE} ({columns}) VALUES ({placeholders})"
    cursor.fast_executemany = True
    cursor.executemany(query, df_pd.values.tolist())
    conn.commit()
    count = len(df_pd)
    cursor.close()
    conn.close()
    return count

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload_and_process")
async def process(file: UploadFile = File(...)):
    try:
        content = await file.read()
        raw = extract_excel_data(content, ['MuestraActual'])
        final = clean_and_transform(raw).with_columns(pl.lit('Web_Render').alias('Origen'))
        rows = load_to_sql(final)
        return {"message": "Éxito", "filas_cargadas": rows, "tabla_destino": DB_TABLE, "servidor_sql": DB_SERVER}
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})

if __name__ == "__main__":
    # Render usa el puerto que le asigne la variable PORT
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)