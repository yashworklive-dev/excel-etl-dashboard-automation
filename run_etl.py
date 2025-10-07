#!/usr/bin/env python3
"""
- Ingest CSV/XLSX files from input/
- Clean dataset
- Produce cleaned_data and dashboard
"""

import os
import glob
from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml

# CONFIG
DEFAULT_CONFIG = {
    "input_folder": "input",
    "output_folder": "output",
    "output_clean_file": "cleaned_data.xlsx",
    "output_dashboard_file": "dashboard.xlsx",
    "col_map": {
        "transaction_id": "transaction_id",
        "transaction_date": "date",
        "transaction_time": "time",
        "transaction_qty": "qty",
        "store_id": "store_id",
        "store_location": "store_location",
        "product_id": "product_id",
        "unit_price": "unit_price",
        "product_category": "category",
        "product_type": "subcategory",
        "product_detail": "product",
        "quantity": "qty",
        "price": "unit_price",
    },
    "category_map": {}
}


def load_config():
    if Path("config.yaml").exists():
        with open("config.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        merged = DEFAULT_CONFIG.copy()
        merged.update({k: v for k, v in cfg.items() if k not in ["col_map", "category_map"]})
        if "col_map" in cfg:
            merged["col_map"].update(cfg["col_map"])
        if "category_map" in cfg:
            merged["category_map"].update(cfg["category_map"])
        return merged
    return DEFAULT_CONFIG


CONFIG = load_config()
INPUT_FOLDER = Path(CONFIG["input_folder"])
OUTPUT_FOLDER = Path(CONFIG["output_folder"])
OUTPUT_CLEAN = OUTPUT_FOLDER / CONFIG["output_clean_file"]
OUTPUT_DASH = OUTPUT_FOLDER / CONFIG["output_dashboard_file"]
COL_MAP = CONFIG["col_map"]
CATEGORY_MAP = CONFIG["category_map"]

# INGEST
def ensure_folders():
    INPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


def ingest_files(folder: Path) -> pd.DataFrame:
    patterns = ["*.xlsx", "*.xls", "*.csv"]
    files = []
    for p in patterns:
        files += glob.glob(str(folder / p))
    files = sorted(files)
    if not files:
        print(f"No input files found in {folder.resolve()}. Place Excel/CSV files there and re-run.")
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            if f.lower().endswith(".csv"):
                df = pd.read_csv(f, low_memory=False)
            else:
                df = pd.read_excel(f, engine="openpyxl")
            df["__source_file"] = Path(f).name
            print(f"Read: {Path(f).name} rows={len(df)}")
            dfs.append(df)
        except Exception as e:
            print(f"Failed to read {f}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# CLEAN & FORMAT
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    lower_map = {k.lower(): v for k, v in COL_MAP.items()}
    new_cols = {}
    for c in df.columns:
        if c in COL_MAP:
            new_cols[c] = COL_MAP[c]
        elif c.lower() in lower_map:
            new_cols[c] = lower_map[c.lower()]
        else:
            new_cols[c] = c.lower().replace(" ", "_")
    return df.rename(columns=new_cols)


def clean_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Trim strings and replace common nulls
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA})
    
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    
    if "time" in df.columns:
        try:
            df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce").dt.time
        except Exception:
            df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.time
    
    for col in ["qty", "unit_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")
    
    if "qty" in df.columns and "unit_price" in df.columns:
        df["sale_amt"] = df["qty"] * df["unit_price"]
    
    if "date" in df.columns:
        df["day"] = df["date"].dt.date
        df["month"] = df["date"].dt.to_period("M").astype(str)
        df["weekday"] = df["date"].dt.day_name()
    
    if "product" in df.columns:
        df["product"] = df["product"].fillna("UNKNOWN_PRODUCT")
    else:
        df["product"] = df.get("product_detail", df.get("product_id", "UNKNOWN_PRODUCT"))
    
    if "category" in df.columns and CATEGORY_MAP:
        df["category"] = df["category"].map(CATEGORY_MAP).fillna(df["category"])
    return df.drop_duplicates()


# AGGREGATE
def compute_aggregates(df: pd.DataFrame) -> dict:
    def safe_group(by, agg):
        try:
            return df.groupby(by).agg(agg).reset_index()
        except Exception:
            return pd.DataFrame()

    total_sales = float(df["sale_amt"].sum()) if "sale_amt" in df.columns else 0.0
    total_transactions = int(len(df))

    monthly = safe_group("month", {"sale_amt": "sum", "qty": "sum"})
    monthly = monthly.rename(columns={"sale_amt": "total_sales", "qty": "units"}).sort_values("month")

    daily = safe_group("day", {"sale_amt": "sum"})
    daily = daily.rename(columns={"sale_amt": "total_sales"}).sort_values("day")

    product = safe_group("product", {"sale_amt": "sum", "qty": "sum"})
    if not product.empty:
        product = product.rename(columns={"sale_amt": "total_sales", "qty": "units"}).sort_values("total_sales", ascending=False)

    category = safe_group("category", {"sale_amt": "sum"})
    if not category.empty:
        category = category.rename(columns={"sale_amt": "total_sales"}).sort_values("total_sales", ascending=False)

    location = safe_group("store_location", {"sale_amt": "sum"})
    if not location.empty:
        location = location.rename(columns={"sale_amt": "total_sales"}).sort_values("total_sales", ascending=False)

    avg_ticket = (total_sales / total_transactions) if total_transactions else 0.0
    top_product = product.iloc[0]["product"] if (not product.empty) else None

    return {
        "kpis": {
            "total_sales": total_sales,
            "total_transactions": total_transactions,
            "avg_ticket": avg_ticket,
            "unique_products": int(df["product"].nunique()) if "product" in df.columns else 0,
            "top_product": top_product,
            "total_units": int(df["qty"].sum()) if "qty" in df.columns else 0
        },
        "monthly": monthly,
        "daily": daily,
        "product": product,
        "category": category,
        "location": location,
    }


# DASHBOARD
def export_cleaned(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False)
    print(f"Wrote cleaned data → {path}")


def create_simple_dashboard(aggs: dict, cleaned_df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd")
    workbook = writer.book

    # helper sheets
    cleaned_df.to_excel(writer, sheet_name="CleanedData", index=False)
    aggs["monthly"].to_excel(writer, sheet_name="Monthly", index=False)
    aggs["daily"].to_excel(writer, sheet_name="Daily", index=False)
    aggs["product"].to_excel(writer, sheet_name="ProductRank", index=False)
    aggs[`"category"].to_excel(writer, sheet_name="Category", index=False)
    aggs["location"].to_excel(writer, sheet_name="Location", index=False)

    # Dashboard sheet
    dash = workbook.add_worksheet("Dashboard")
    dash.hide_gridlines(2)
    dash.set_zoom(110)
    dash.set_column("A:L", 16)
    for r in range(0, 60):
        dash.set_row(r, 20)

    # Formats
    title_fmt = workbook.add_format({"bold": True, "font_size": 18})
    subtitle_fmt = workbook.add_format({"font_size": 10, "italic": True, "font_color": "#666666"})
    kpi_label_fmt = workbook.add_format({"bold": True, "font_size": 9})
    kpi_value_big = workbook.add_format({"bold": True, "font_size": 18, "num_format": "#,##0"})
    kpi_value_money = workbook.add_format({"bold": True, "font_size": 18, "num_format": "₹#,##0.00"})
    small_muted = workbook.add_format({"font_size": 9, "font_color": "#666666"})
    section_header = workbook.add_format({"bold": True, "font_size": 11})

    # Title
    dash.merge_range("A1:F1", "Coffee Shop Sales Dashboard", title_fmt)
    dash.merge_range("A2:F2", "Automated ETL output — drop files into input/ and run", subtitle_fmt)

    kpi_positions = [
        ("A4", "A5"),  # KPI 1
        ("C4", "C5"),  # KPI 2
        ("E4", "E5"),  # KPI 3
        ("A7", "A8"),  # KPI 4
        ("C7", "C8"),  # KPI 5
        ("E7", "E8"),  # KPI 6
    ]

    k = aggs["kpis"]
    kpi_texts = [
        ("Total Sales", k.get("total_sales", 0.0), "money"),
        ("Total Transactions", k.get("total_transactions", 0), "num"),
        ("Average Ticket", k.get("avg_ticket", 0.0), "money"),
        ("Unique Products", k.get("unique_products", 0), "num"),
        ("Total Units Sold", k.get("total_units", 0), "num"),
        ("Top Product", k.get("top_product", "N/A"), "text")
    ]

    # KPI labels and values
    for (label_cell, value_cell), (label, value, vtype) in zip(kpi_positions, kpi_texts):
        dash.write(label_cell, label, kpi_label_fmt)
        if vtype == "money":
            dash.write(value_cell, value, kpi_value_money)
        elif vtype == "num":
            dash.write(value_cell, value, kpi_value_big)
        else:
            dash.write(value_cell, value, workbook.add_format({"bold": True, "font_size": 14}))

    # Section headers
    dash.write("A10", "Monthly Sales", section_header)
    dash.write("E10", "Daily Trend", section_header)
    dash.write("A26", "Sales by Category", section_header)
    dash.write("E26", "Sales by Store", section_header)
    dash.write("A42", "Top Products", section_header)


    # Monthly chart
    if not aggs["monthly"].empty:
        mrows = len(aggs["monthly"])
        ch = workbook.add_chart({"type": "column"})
        ch.add_series({
            "name": "Monthly Sales",
            "categories": ["Monthly", 1, 0, mrows, 0],
            "values": ["Monthly", 1, 1, mrows, 1],
        })
        ch.set_title({"name": ""})
        ch.set_legend({"position": "none"})
        dash.insert_chart("A12", ch, {"x_scale": 1.4, "y_scale": 1.1})

    
    if not aggs["daily"].empty:
        drows = len(aggs["daily"])
        ch2 = workbook.add_chart({"type": "line"})
        ch2.add_series({
            "name": "Daily Sales",
            "categories": ["Daily", 1, 0, drows, 0],
            "values": ["Daily", 1, 1, drows, 1],
        })
        ch2.set_title({"name": ""})
        ch2.set_legend({"position": "none"})
        dash.insert_chart("E12", ch2, {"x_scale": 1.4, "y_scale": 1.1})


    if not aggs["category"].empty:
        crow = min(len(aggs["category"]), 10)
        ch3 = workbook.add_chart({"type": "bar"})
        ch3.add_series({
            "name": "Category Sales",
            "categories": ["Category", 1, 0, crow, 0],
            "values": ["Category", 1, 1, crow, 1],
        })
        ch3.set_title({"name": ""})
        ch3.set_legend({"position": "none"})
        dash.insert_chart("A28", ch3, {"x_scale": 1.2, "y_scale": 0.9})

   
    if not aggs["location"].empty:
        lrows = min(len(aggs["location"]), 10)
        ch4 = workbook.add_chart({"type": "column"})
        ch4.add_series({
            "name": "Sales by Store",
            "categories": ["Location", 1, 0, lrows, 0],
            "values": ["Location", 1, 1, lrows, 1],
        })
        ch4.set_title({"name": ""})
        ch4.set_legend({"position": "none"})
        dash.insert_chart("E28", ch4, {"x_scale": 1.2, "y_scale": 0.9})

   
    if not aggs["product"].empty:
        prow = min(10, len(aggs["product"]))
        ch5 = workbook.add_chart({"type": "bar"})
        ch5.add_series({
            "name": "Top Products",
            "categories": ["ProductRank", 1, 0, prow, 0],
            "values": ["ProductRank", 1, 1, prow, 1],
        })
        ch5.set_title({"name": ""})
        ch5.set_legend({"position": "none"})
        dash.insert_chart("A44", ch5, {"x_scale": 1.4, "y_scale": 1.0})

    # Footer
    footer_fmt = workbook.add_format({"italic": True, "font_size": 9, "font_color": "#666666"})
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dash.write("A58", f"Generated on {ts} • run_etl.py", footer_fmt)

    
    for sheet in ["Monthly", "Daily", "ProductRank", "Category", "Location", "CleanedData"]:
        try:
            workbook.get_worksheet_by_name(sheet).hide()
        except Exception:
            pass

    writer.close()
    print(f"Wrote simple dashboard → {out_path}")


# MAIN
def main():
    start = datetime.now()
    ensure_folders()
    raw = ingest_files(INPUT_FOLDER)
    if raw.empty:
        return
    norm = normalize_columns(raw)
    clean = clean_and_enrich(norm)
    export_cleaned(clean, OUTPUT_CLEAN)
    aggs = compute_aggregates(clean)
    create_simple_dashboard(aggs, clean, OUTPUT_DASH)
    print(f"ETL finished in {datetime.now() - start}")


if __name__ == "__main__":
    main()
