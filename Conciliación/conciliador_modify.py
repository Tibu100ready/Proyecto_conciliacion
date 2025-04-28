import pandas as pd
from datetime import datetime, timedelta
import random
from pathlib import Path

# --- 1. Generar datos ficticios para banco y ERP ---
def generar_datos_conciliacion(n=4100, diferencias=900):
    # Generar las fechas y montos
    fechas_base = [datetime.today() - timedelta(days=random.randint(0, 5000)) for _ in range(n)]
    montos_base = [round(random.uniform(100, 5000), 2) for _ in range(n)]
    
    # Crear un conjunto de referencias únicas
    referencias_base = [f"{random.randint(10000, 99999)}" for _ in range(n)]

    # Crear DataFrame para banco
    banco = pd.DataFrame({
        "fecha": fechas_base,
        "monto": montos_base,
        "referencia": referencias_base
    })

    # Crear un DataFrame para ERP asegurando que las referencias sean las mismas que en banco
    erp = banco.copy()

    for _ in range(diferencias):
        # Crear un nuevo DataFrame con los datos diferentes
        nuevo_banco = pd.DataFrame({
            "fecha": [datetime.today() - timedelta(days=random.randint(0, 300))],
            "monto": [round(random.uniform(100, 5000), 2)],
            "referencia": [f"{random.randint(10000, 99999)}"]
        })
        
        nuevo_erp = pd.DataFrame({
            "fecha": [datetime.today() - timedelta(days=random.randint(0, 300))],
            "monto": [round(random.uniform(100, 5000), 2)],
            "referencia": [f"{random.randint(10000, 99999)}"]
        })
        
        # Concatenar los nuevos datos
        banco = pd.concat([banco, nuevo_banco], ignore_index=True)
        erp = pd.concat([erp, nuevo_erp], ignore_index=True)

    # Convertir las fechas a formato dd/mm/aaaa
    banco['fecha'] = banco['fecha'].dt.strftime('%d/%m/%Y')
    erp['fecha'] = erp['fecha'].dt.strftime('%d/%m/%Y')

    # Guardar los archivos CSV
    banco.to_csv("banco.csv", index=False)
    erp.to_csv("erp.csv", index=False)
    print("✅ Archivos 'banco.csv' y 'erp.csv' generados.")

# --- 2. Conciliación ---
def conciliar():
    banco = pd.read_csv('banco.csv', parse_dates=['fecha'], dayfirst=True)
    erp = pd.read_csv('erp.csv', parse_dates=['fecha'], dayfirst=True)

    banco['monto'] = banco['monto'].astype(float)
    erp['monto'] = erp['monto'].astype(float)

    banco['referencia'] = banco['referencia'].astype(str).str.strip()
    erp['referencia'] = erp['referencia'].astype(str).str.strip()

    # Conciliar las tablas por monto, fecha y referencia
    conciliados = pd.merge(banco, erp, on=['monto', 'fecha', 'referencia'], how='inner')

    banco_ids = banco.apply(lambda row: f"{row['monto']}_{row['fecha']}_{row['referencia']}", axis=1)
    erp_ids = erp.apply(lambda row: f"{row['monto']}_{row['fecha']}_{row['referencia']}", axis=1)
    conciliados_ids = conciliados.apply(lambda row: f"{row['monto']}_{row['fecha']}_{row['referencia']}", axis=1)

    no_conciliados_banco = banco[~banco_ids.isin(conciliados_ids)]
    no_conciliados_erp = erp[~erp_ids.isin(conciliados_ids)]

    # Crear la carpeta 'resultados' si no existe
    Path("resultados").mkdir(exist_ok=True)
    
    # Guardar los resultados
    conciliados.to_csv("resultados/conciliados.csv", index=False)
    no_conciliados_banco.to_csv("resultados/no_conciliados_banco.csv", index=False)
    no_conciliados_erp.to_csv("resultados/no_conciliados_erp.csv", index=False)

    print("✅ Conciliación completada. Revisa la carpeta 'resultados'.")

# --- 3. Ejecutar todo ---
if __name__ == "__main__":
    generar_datos_conciliacion()
    conciliar()
