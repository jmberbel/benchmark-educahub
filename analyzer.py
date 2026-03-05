"""
analyzer.py — Excel sales analysis module for BenchmarkHub.
Processes uploaded .xlsx files with pandas to extract KPIs,
top products, emerging/declining trends, and faculty breakdowns.
"""

import pandas as pd
import warnings
import logging

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


def analyze_sales(filepath: str) -> dict:
    """
    Analyze a sales Excel file and return structured results.
    Adapts to different column structures automatically.
    """
    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return {"error": f"No se pudo leer el archivo Excel: {str(e)}"}

    results = {
        "columns": list(df.columns),
        "total_rows": len(df),
        "kpis": {},
        "top_20": [],
        "emerging": [],
        "declining": [],
        "dead_products": [],
        "by_faculty": [],
        "by_school": [],
        "year_columns": [],
        "raw_summary": {},
    }

    # Detect year columns
    year_cols = [
        c
        for c in df.columns
        if any(str(y) in str(c) for y in range(2018, 2030))
    ]
    results["year_columns"] = year_cols

    if not year_cols:
        results["error"] = (
            "No se encontraron columnas de ventas por año. "
            "El archivo debe tener columnas con años (2020, 2021, etc.)."
        )
        return results

    # Convert year columns to numeric
    for col in year_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Total sales
    df["ventas_total"] = df[year_cols].sum(axis=1)

    # Growth calculation
    if len(year_cols) >= 2:
        last_yr = year_cols[-1]
        prev_yr = year_cols[-2]
        df["crecimiento_pct"] = (
            (df[last_yr] - df[prev_yr]) / df[prev_yr].replace(0, float("nan"))
        ) * 100
        df["crecimiento_pct"] = df["crecimiento_pct"].fillna(0).round(1)

    # Detect name column
    name_col = None
    for candidate in [
        "Producto",
        "Nombre",
        "Nombre del Producto",
        "Programa",
        "Titulo",
        "Título",
        "Denominación",
        "Denominacion",
    ]:
        if candidate in df.columns:
            name_col = candidate
            break
    if not name_col:
        name_col = df.columns[0]
    results["name_column"] = name_col

    # KPIs
    results["kpis"] = {
        "total_ventas": int(df["ventas_total"].sum()),
        "total_productos": len(df),
        "productos_activos": int((df["ventas_total"] > 0).sum()),
        "productos_muertos": int((df["ventas_total"] == 0).sum()),
        "producto_top": (
            str(df.loc[df["ventas_total"].idxmax(), name_col]) if len(df) > 0 else ""
        ),
        "ventas_producto_top": int(df["ventas_total"].max()) if len(df) > 0 else 0,
    }

    if "crecimiento_pct" in df.columns:
        valid_growth = df[
            df["crecimiento_pct"].notna() & (df["crecimiento_pct"] != 0)
        ]
        results["kpis"]["crecimiento_medio"] = (
            round(valid_growth["crecimiento_pct"].mean(), 1)
            if len(valid_growth) > 0
            else 0
        )

    for col in year_cols:
        results["kpis"][f"ventas_{col}"] = int(df[col].sum())

    # Build display columns
    show_cols = [
        c
        for c in [
            "Facultad",
            "Escuela",
            "Institución Educativa",
            name_col,
            "Tipo",
            "Precio",
            "Horas",
        ]
        if c in df.columns
    ]
    show_cols += year_cols + ["ventas_total"]
    if "crecimiento_pct" in df.columns:
        show_cols.append("crecimiento_pct")

    # Deduplicate show_cols preserving order
    seen = set()
    unique_show_cols = []
    for c in show_cols:
        if c not in seen:
            seen.add(c)
            unique_show_cols.append(c)
    show_cols = unique_show_cols

    # Top 20
    top20 = df.nlargest(20, "ventas_total")
    results["top_20"] = _df_to_records(top20, show_cols)

    # Emerging (>15% growth)
    if "crecimiento_pct" in df.columns and len(year_cols) >= 2:
        last_yr = year_cols[-1]
        emerging = df[
            (df["crecimiento_pct"] > 15) & (df[last_yr] > 0)
        ].nlargest(10, "crecimiento_pct")
        results["emerging"] = _df_to_records(emerging, show_cols)

        declining = df[df["crecimiento_pct"] < -15].nsmallest(
            10, "crecimiento_pct"
        )
        results["declining"] = _df_to_records(declining, show_cols)

    # Dead products
    if len(year_cols) >= 2:
        recent_cols = year_cols[-2:]
        dead = df[df[recent_cols].sum(axis=1) == 0]
        dead_cols = [
            c for c in [name_col, "Tipo", "Facultad", "Escuela"] if c in df.columns
        ]
        results["dead_products"] = dead[dead_cols].head(50).to_dict("records")
        results["kpis"]["productos_muertos_recientes"] = len(dead)

    # By faculty
    if "Facultad" in df.columns:
        fac = (
            df.groupby("Facultad")["ventas_total"]
            .agg(["sum", "count", "mean"])
            .sort_values("sum", ascending=False)
        )
        fac.columns = ["ventas_totales", "n_productos", "media_ventas"]
        results["by_faculty"] = fac.round(1).reset_index().to_dict("records")

    # By school
    if "Escuela" in df.columns:
        esc = (
            df.groupby("Escuela")["ventas_total"]
            .agg(["sum", "count", "mean"])
            .sort_values("sum", ascending=False)
        )
        esc.columns = ["ventas_totales", "n_productos", "media_ventas"]
        results["by_school"] = esc.round(1).reset_index().to_dict("records")

    # Raw summary for Claude context
    results["raw_summary"] = {
        "all_products_count": len(df),
        "columns_detected": list(df.columns),
        "year_range": f"{year_cols[0]} - {year_cols[-1]}",
        "sample_products": _df_to_records(df.head(5), show_cols),
    }

    return results


def _df_to_records(df: pd.DataFrame, cols: list) -> list:
    """Convert DataFrame to list of dicts, handling NaN and type conversion."""
    available_cols = [c for c in cols if c in df.columns]
    records = df[available_cols].to_dict("records")
    # Clean up NaN values
    for r in records:
        for k, v in r.items():
            if pd.isna(v):
                r[k] = None
            elif isinstance(v, float) and v == int(v):
                r[k] = int(v)
    return records
