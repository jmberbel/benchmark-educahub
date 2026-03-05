"""
analyzer.py — Excel sales analysis module for BenchmarkHub.
Processes uploaded .xlsx files with pandas to extract KPIs,
top products, emerging/declining trends, and faculty breakdowns.

Handles multiple Excel formats:
- Standard headers in row 1
- Headers offset (e.g., row 2, 3, or deeper) with merged cells
- Year columns as "2023", "2024" or "Matrículas 23", "Importe 23", etc.
"""

import pandas as pd
import numpy as np
import re
import warnings
import logging

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


def _detect_header_row(filepath: str, max_scan: int = 10) -> int:
    """
    Scan the first `max_scan` rows of an Excel file to find the actual
    header row. Looks for rows with multiple non-null string values and
    keywords typical of product/sales headers.
    """
    try:
        df_raw = pd.read_excel(filepath, header=None, nrows=max_scan)
    except Exception:
        return 0

    keywords = [
        "producto", "nombre", "programa", "título", "titulo", "denominación",
        "denominacion", "idioma", "tipo", "facultad", "escuela", "precio",
        "horas", "créditos", "creditos", "matrícula", "matricula",
        "importe", "ventas", "ingresos",
    ]

    best_row = 0
    best_score = 0

    for idx in range(len(df_raw)):
        row_values = df_raw.iloc[idx].dropna().astype(str).str.lower().tolist()
        if len(row_values) < 2:
            continue
        score = sum(1 for v in row_values if any(kw in v for kw in keywords))
        # Also boost rows that look like they have year-like columns
        score += sum(1 for v in row_values if re.search(r"(?:20\d{2}|\b\d{2}\b)", v))
        if score > best_score:
            best_score = score
            best_row = idx

    return best_row


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean up column names: strip whitespace, remove unnamed columns,
    and replace fully empty first columns.
    """
    # Strip whitespace from column names
    df.columns = [str(c).strip() for c in df.columns]

    # Drop columns that are fully unnamed/empty
    drop_cols = [c for c in df.columns if c.startswith("Unnamed") and df[c].isna().all()]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # If first column is Unnamed but has data, try to give it a name
    if df.columns[0].startswith("Unnamed"):
        # Check if it looks like a product name column
        sample = df.iloc[:5, 0].dropna()
        if len(sample) > 0 and all(isinstance(v, str) for v in sample):
            df = df.rename(columns={df.columns[0]: "Producto"})

    return df


def _detect_year_columns(df: pd.DataFrame) -> dict:
    """
    Detect year-related columns. Handles:
    - Direct year columns: "2023", "2024", "2025"
    - Prefixed columns: "Matrículas 23", "Importe 23", "Ventas 2024"
    - Returns a dict with: {
        'matricula_cols': {year: col_name, ...},
        'importe_cols': {year: col_name, ...},
        'year_cols': [col_names for sales/matriculas],
        'years_detected': [2023, 2024, ...]
      }
    """
    result = {
        "matricula_cols": {},
        "importe_cols": {},
        "year_cols": [],
        "years_detected": [],
    }

    matricula_pattern = re.compile(
        r"(?:matr[ií]cula|ventas?|alumnos?|inscrip)\w*\s*(\d{2,4})",
        re.IGNORECASE,
    )
    importe_pattern = re.compile(
        r"(?:importe|ingreso|revenue|factura)\w*\s*(\d{2,4})",
        re.IGNORECASE,
    )
    direct_year_pattern = re.compile(r"^(20\d{2})$")

    for col in df.columns:
        col_str = str(col).strip()

        # Direct year column (e.g., "2023")
        m = direct_year_pattern.match(col_str)
        if m:
            year = int(m.group(1))
            result["matricula_cols"][year] = col
            result["years_detected"].append(year)
            continue

        # Matricula/ventas columns (e.g., "Matrículas 23", "Ventas 2024")
        m = matricula_pattern.search(col_str)
        if m:
            yr = int(m.group(1))
            year = yr if yr > 100 else 2000 + yr
            result["matricula_cols"][year] = col
            if year not in result["years_detected"]:
                result["years_detected"].append(year)
            continue

        # Importe/revenue columns (e.g., "Importe 23", "Ingresos 2024")
        m = importe_pattern.search(col_str)
        if m:
            yr = int(m.group(1))
            year = yr if yr > 100 else 2000 + yr
            result["importe_cols"][year] = col
            if year not in result["years_detected"]:
                result["years_detected"].append(year)
            continue

        # Fallback: any column containing a year-like number
        for y in range(2018, 2035):
            if str(y) in col_str:
                # Determine type by context
                cl = col_str.lower()
                if any(kw in cl for kw in ["importe", "ingreso", "revenue", "factura"]):
                    result["importe_cols"][y] = col
                else:
                    result["matricula_cols"][y] = col
                if y not in result["years_detected"]:
                    result["years_detected"].append(y)
                break

    result["years_detected"].sort()
    # Year cols = the matricula/sales columns in order
    result["year_cols"] = [
        result["matricula_cols"][y]
        for y in sorted(result["matricula_cols"].keys())
    ]

    return result


def analyze_sales(filepath: str) -> dict:
    """
    Analyze a sales Excel file and return structured results.
    Adapts to different column structures automatically.
    """
    # Step 1: Detect the header row
    header_row = _detect_header_row(filepath)
    logger.info(f"Detected header at row {header_row}")

    try:
        df = pd.read_excel(filepath, header=header_row)
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return {"error": f"No se pudo leer el archivo Excel: {str(e)}"}

    # Step 2: Normalize columns
    df = _normalize_columns(df)

    # Remove summary/total rows (rows where the name contains "total")
    name_col_candidates = [
        c for c in df.columns
        if not c.startswith("Unnamed") and df[c].dtype == object
    ]
    if name_col_candidates:
        first_text_col = name_col_candidates[0]
        df = df[~df[first_text_col].astype(str).str.lower().str.contains("total", na=False)]

    # Remove fully empty rows
    df = df.dropna(how="all").reset_index(drop=True)

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
        "importe_columns": [],
        "raw_summary": {},
    }

    # Step 3: Detect year columns
    yr_info = _detect_year_columns(df)
    year_cols = yr_info["year_cols"]
    importe_cols = [yr_info["importe_cols"].get(y) for y in sorted(yr_info["importe_cols"].keys())]
    importe_cols = [c for c in importe_cols if c is not None]
    years_detected = yr_info["years_detected"]

    results["year_columns"] = year_cols
    results["importe_columns"] = importe_cols
    results["years_detected"] = years_detected

    if not year_cols:
        results["error"] = (
            "No se encontraron columnas de ventas/matrículas por año. "
            "El archivo debe tener columnas con años (2020, 2021, Matrículas 23, etc.)."
        )
        return results

    # Step 4: Convert year columns to numeric
    for col in year_cols + importe_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Step 5: Calculate totals
    df["ventas_total"] = df[year_cols].sum(axis=1)
    if importe_cols:
        df["importe_total"] = df[importe_cols].sum(axis=1)

    # Step 6: Growth calculation
    if len(year_cols) >= 2:
        last_yr = year_cols[-1]
        prev_yr = year_cols[-2]
        df["crecimiento_pct"] = (
            (df[last_yr] - df[prev_yr]) / df[prev_yr].replace(0, float("nan"))
        ) * 100
        df["crecimiento_pct"] = df["crecimiento_pct"].fillna(0).round(1)

    # Also calculate importe growth if available
    if len(importe_cols) >= 2:
        last_imp = importe_cols[-1]
        prev_imp = importe_cols[-2]
        df["crecimiento_importe_pct"] = (
            (df[last_imp] - df[prev_imp]) / df[prev_imp].replace(0, float("nan"))
        ) * 100
        df["crecimiento_importe_pct"] = df["crecimiento_importe_pct"].fillna(0).round(1)

    # Step 7: Detect name column
    name_col = None
    for candidate in [
        "Producto", "Nombre", "Nombre del Producto", "Programa",
        "Titulo", "Título", "Denominación", "Denominacion",
        "IDIOMA", "Idioma", "Categoría", "Categoria",
    ]:
        if candidate in df.columns:
            name_col = candidate
            break
    if not name_col:
        # Use first non-numeric, non-unnamed column
        for c in df.columns:
            if not c.startswith("Unnamed") and df[c].dtype == object:
                name_col = c
                break
    if not name_col:
        name_col = df.columns[0]
    results["name_column"] = name_col

    # Step 8: KPIs
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

    if importe_cols:
        results["kpis"]["total_importe"] = round(float(df["importe_total"].sum()), 2)

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
        # Create a clean label for the year
        yr_match = re.search(r"(\d{2,4})", str(col))
        yr_label = yr_match.group(1) if yr_match else col
        if len(yr_label) == 2:
            yr_label = f"20{yr_label}"
        results["kpis"][f"ventas_{yr_label}"] = int(df[col].sum())

    for col in importe_cols:
        yr_match = re.search(r"(\d{2,4})", str(col))
        yr_label = yr_match.group(1) if yr_match else col
        if len(yr_label) == 2:
            yr_label = f"20{yr_label}"
        results["kpis"][f"importe_{yr_label}"] = round(float(df[col].sum()), 2)

    # Step 9: Build display columns
    show_cols = [
        c
        for c in [
            "Facultad", "Escuela", "Institución Educativa",
            name_col, "Tipo", "Precio", "Horas", "Créditos",
        ]
        if c in df.columns
    ]
    show_cols += year_cols
    if importe_cols:
        show_cols += importe_cols
    show_cols += ["ventas_total"]
    if "importe_total" in df.columns:
        show_cols.append("importe_total")
    if "crecimiento_pct" in df.columns:
        show_cols.append("crecimiento_pct")
    if "crecimiento_importe_pct" in df.columns:
        show_cols.append("crecimiento_importe_pct")

    # Deduplicate preserving order
    seen = set()
    unique_show_cols = []
    for c in show_cols:
        if c not in seen:
            seen.add(c)
            unique_show_cols.append(c)
    show_cols = unique_show_cols

    # Step 10: Top 20
    top20 = df.nlargest(20, "ventas_total")
    results["top_20"] = _df_to_records(top20, show_cols)

    # Step 11: Emerging (>15% growth)
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

    # Step 12: Dead products
    if len(year_cols) >= 2:
        recent_cols = year_cols[-2:]
        dead = df[df[recent_cols].sum(axis=1) == 0]
        dead_cols = [
            c for c in [name_col, "Tipo", "Facultad", "Escuela"] if c in df.columns
        ]
        results["dead_products"] = dead[dead_cols].head(50).to_dict("records")
        results["kpis"]["productos_muertos_recientes"] = len(dead)

    # Step 13: By faculty
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

    # Step 14: Raw summary for Claude context
    results["raw_summary"] = {
        "all_products_count": len(df),
        "columns_detected": list(df.columns),
        "year_range": f"{years_detected[0]} - {years_detected[-1]}" if years_detected else "N/A",
        "sample_products": _df_to_records(df.head(5), show_cols),
    }

    return results


def _df_to_records(df: pd.DataFrame, cols: list) -> list:
    """Convert DataFrame to list of dicts, handling NaN and type conversion."""
    available_cols = [c for c in cols if c in df.columns]
    records = df[available_cols].to_dict("records")
    for r in records:
        for k, v in r.items():
            if isinstance(v, (float, np.floating)) and (pd.isna(v) or np.isnan(v)):
                r[k] = None
            elif isinstance(v, (np.integer,)):
                r[k] = int(v)
            elif isinstance(v, (float, np.floating)):
                fv = round(float(v), 2)
                r[k] = int(fv) if fv == int(fv) else fv
    return records
