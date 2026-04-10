import io
import logging
import pandas as pd
import numpy as np
import json
import datetime
from typing import List, Any, Dict
from flask import Flask, request, jsonify, render_template
from agent import get_header_decision, get_model_name
from db_storage import init_db, store_excel_data, update_file_result_json, get_db_connection
from summarizer_agent import summarize_file

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'xlsm'}

# Cache for file bytes (for re‑parsing after corrections)
file_bytes_cache = {}

init_db()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def convert_to_serializable(obj: Any) -> Any:
    """Recursively convert all non‑JSON‑serializable objects to serializable types."""
    if obj is None:
        return None
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, np.ndarray):
        return convert_to_serializable(obj.tolist())
    if isinstance(obj, pd.Series):
        return convert_to_serializable(obj.tolist())
    if hasattr(obj, 'dtype') and np.isscalar(obj):
        try:
            if pd.isna(obj):
                return None
        except (ValueError, TypeError):
            pass
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        pass
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]
    return obj

def build_nested_columns(header_data: pd.DataFrame, header_rows: int) -> List[List]:
    if header_data.empty:
        return []
    filled_rows = []
    for row_idx in range(header_rows):
        row = header_data.iloc[row_idx].tolist()
        last_val = None
        for i in range(len(row)):
            if pd.isna(row[i]) or row[i] is None:
                row[i] = last_val
            else:
                last_val = row[i]
        filled_rows.append(row)
    num_cols = len(filled_rows[0]) if filled_rows else 0
    columns = []
    for col_idx in range(num_cols):
        col_hierarchy = [filled_rows[row_idx][col_idx] for row_idx in range(header_rows)]
        columns.append(col_hierarchy)
    return columns

def is_empty_or_irrelevant(preview_rows: List[List[Any]], sheet_name: str):
    if not preview_rows or len(preview_rows) == 0:
        return True, "Sheet is completely empty"
    all_empty = True
    for row in preview_rows:
        if row and any(cell is not None for cell in row):
            all_empty = False
            break
    if all_empty:
        return True, "Sheet contains no data (all cells empty)"
    meaningful_cells = 0
    for row in preview_rows[:5]:
        for cell in row:
            if cell is not None:
                if isinstance(cell, str) and len(cell.strip()) > 0:
                    meaningful_cells += 1
                elif not isinstance(cell, str):
                    meaningful_cells += 1
    if meaningful_cells == 0:
        return True, "Sheet contains only empty or whitespace cells"
    return False, ""

def are_rows_empty(file_bytes: bytes, sheet_name: str, skiprows: int, num_rows: int = 5) -> bool:
    try:
        data_df = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=sheet_name,
            header=None,
            skiprows=skiprows,
            nrows=num_rows
        )
        if data_df.empty:
            return True
        for _, row in data_df.iterrows():
            if any(not pd.isna(cell) for cell in row):
                return False
        return True
    except Exception as e:
        logger.warning(f"Error checking data rows for sheet '{sheet_name}': {e}")
        return True

def get_preview_headers(file_bytes: bytes, sheet_name: str, start_row: int, header_rows: int) -> List:
    try:
        if header_rows == 0:
            return []
        header_data = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=sheet_name,
            header=None,
            skiprows=start_row,
            nrows=header_rows
        )
        if header_data.empty:
            return []
        if header_rows == 1:
            row = header_data.iloc[0].tolist()
            last_val = None
            for i in range(len(row)):
                if pd.isna(row[i]) or row[i] is None:
                    row[i] = last_val
                else:
                    last_val = row[i]
            return row
        else:
            return build_nested_columns(header_data, header_rows)
    except Exception as e:
        logger.error(f"Preview headers failed: {e}")
        return []

def parse_excel_with_decisions(file_bytes: bytes, corrections: Dict[str, Dict] = None, skip_sheets: List[str] = None):
    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
    sheets_info = []
    data_rows_by_sheet = {}

    if skip_sheets is None:
        skip_sheets = []

    for sheet_name in excel_file.sheet_names:
        if sheet_name in skip_sheets:
            sheets_info.append({
                'sheet_name': sheet_name,
                'skipped': True,
                'skip_reason': "Manually skipped by user"
            })
            continue

        logger.info(f"Processing sheet: {sheet_name}")

        preview_df = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=sheet_name,
            header=None,
            nrows=10
        )
        preview_rows = []
        for _, row in preview_df.iterrows():
            row_list = [None if pd.isna(cell) else cell for cell in row.tolist()]
            preview_rows.append(row_list)

        is_irrelevant, reason = is_empty_or_irrelevant(preview_rows, sheet_name)
        if is_irrelevant:
            sheets_info.append({'sheet_name': sheet_name, 'skipped': True, 'skip_reason': reason})
            continue

        if corrections and sheet_name in corrections:
            start_row = corrections[sheet_name]["header_start_row"]
            header_rows = corrections[sheet_name]["header_rows_count"]
            nested = header_rows >= 2
            logger.info(f"Using correction for {sheet_name}: start_row={start_row}, header_rows={header_rows}")
        else:
            start_row, header_rows, nested = get_header_decision(sheet_name, preview_rows)
            logger.info(f"AI decision for {sheet_name}: start_row={start_row}, header_rows={header_rows}")

        rows_to_skip = start_row + header_rows
        if are_rows_empty(file_bytes, sheet_name, rows_to_skip, num_rows=5):
            sheets_info.append({
                'sheet_name': sheet_name,
                'skipped': True,
                'skip_reason': "No data rows after headers (first 5 rows empty)"
            })
            continue

        # Extract columns
        if header_rows == 0:
            data_df = pd.read_excel(
                io.BytesIO(file_bytes),
                sheet_name=sheet_name,
                header=None,
                skiprows=start_row,
                nrows=1
            )
            col_count = data_df.shape[1] if data_df.shape[0] > 0 else 0
            if col_count == 0:
                sheets_info.append({'sheet_name': sheet_name, 'skipped': True, 'skip_reason': "No columns found"})
                continue
            columns = [f"Column_{i+1}" for i in range(col_count)]
        else:
            header_data = pd.read_excel(
                io.BytesIO(file_bytes),
                sheet_name=sheet_name,
                header=None,
                skiprows=start_row,
                nrows=header_rows
            )
            if header_data.empty or header_data.shape[1] == 0:
                sheets_info.append({'sheet_name': sheet_name, 'skipped': True, 'skip_reason': "No column headers found"})
                continue
            if header_rows == 1:
                row = header_data.iloc[0].tolist()
                last_val = None
                for i in range(len(row)):
                    if pd.isna(row[i]) or row[i] is None:
                        row[i] = last_val
                    else:
                        last_val = row[i]
                columns = row
            else:
                columns = build_nested_columns(header_data, header_rows)

        # Data rows
        data_start_row = start_row + header_rows
        data_df = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=sheet_name,
            header=None,
            skiprows=data_start_row,
            nrows=10000
        )
        data_rows = []
        for _, row in data_df.iterrows():
            row_list = [None if pd.isna(cell) else cell for cell in row.tolist()]
            data_rows.append(row_list)
        data_rows_by_sheet[sheet_name] = data_rows
        first_data_rows_preview = data_rows[:3]

        sheets_info.append({
            'sheet_name': sheet_name,
            'skipped': False,
            'ai_decision': {
                'header_start_row': start_row,
                'header_rows_count': header_rows,
                'nested_structure': nested
            },
            'columns': columns,
            'first_data_rows_preview': first_data_rows_preview,
            'preview_rows': preview_rows
        })

    return sheets_info, data_rows_by_sheet

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if not allowed_file(file.filename):
        return jsonify({'error': 'File type not allowed'}), 400

    try:
        file_bytes = file.read()
        if len(file_bytes) == 0:
            return jsonify({'error': 'Empty file'}), 400

        sheets_info, data_rows_by_sheet = parse_excel_with_decisions(file_bytes, corrections=None)

        file_hash = store_excel_data(file_bytes, file.filename, get_model_name(), sheets_info, data_rows_by_sheet)
        file_bytes_cache[file_hash] = file_bytes

        try:
            summary = summarize_file(file_hash, save=True)
        except Exception as e:
            logger.error(f"Summary generation failed: {e}")
            summary = None

        response_sheets = []
        for s in sheets_info:
            resp = {
                'sheet_name': s['sheet_name'],
                'skipped': s.get('skipped', False),
                'ai_decision': s.get('ai_decision', {}),
                'columns': s.get('columns', []),
                'preview_rows': s.get('preview_rows', [])
            }
            if not s.get('skipped'):
                resp['first_data_rows_preview'] = s.get('first_data_rows_preview', [])
            response_sheets.append(resp)

        response = {
            'filename': file.filename,
            'model_used': get_model_name(),
            'file_hash': file_hash,
            'summary': summary,
            'sheets': response_sheets
        }
        response_serializable = convert_to_serializable(response)
        update_file_result_json(file_hash, json.dumps(response_serializable, ensure_ascii=False))
        return jsonify(response_serializable), 200

    except Exception as e:
        logger.exception("Error parsing Excel file")
        return jsonify({'error': f'Failed to parse Excel file: {str(e)}'}), 400

@app.route('/apply_corrections', methods=['POST'])
def apply_corrections():
    data = request.get_json()
    file_hash = data.get("file_hash")
    corrections = data.get("corrections", [])

    if not file_hash:
        return jsonify({"error": "Missing file_hash"}), 400

    if file_hash not in file_bytes_cache:
        return jsonify({"error": "File bytes not found. Please re-upload."}), 404

    file_bytes = file_bytes_cache[file_hash]

    # Retrieve original filename from the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM files WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    conn.close()
    original_filename = row["filename"] if row else "unknown.xlsx"

    # Separate skipped sheets and header corrections
    skipped_sheets = []
    header_corrections = {}
    for corr in corrections:
        sheet_name = corr["sheet_name"]
        if corr.get("skipped", False):
            skipped_sheets.append(sheet_name)
        else:
            header_corrections[sheet_name] = {
                "header_start_row": corr["header_start_row"],
                "header_rows_count": corr["header_rows_count"]
            }

    try:
        sheets_info, data_rows_by_sheet = parse_excel_with_decisions(
            file_bytes,
            corrections=header_corrections,
            skip_sheets=skipped_sheets
        )
        store_excel_data(file_bytes, original_filename, get_model_name(), sheets_info, data_rows_by_sheet)

        try:
            summary = summarize_file(file_hash, save=True)
        except Exception as e:
            logger.error(f"Summary generation failed: {e}")
            summary = None

        response_sheets = []
        for s in sheets_info:
            resp = {
                'sheet_name': s['sheet_name'],
                'skipped': s.get('skipped', False),
                'ai_decision': s.get('ai_decision', {}),
                'columns': s.get('columns', []),
                'preview_rows': s.get('preview_rows', [])
            }
            if not s.get('skipped'):
                resp['first_data_rows_preview'] = s.get('first_data_rows_preview', [])
            response_sheets.append(resp)

        response = {
            'filename': original_filename,
            'model_used': get_model_name(),
            'file_hash': file_hash,
            'summary': summary,
            'sheets': response_sheets
        }
        response_serializable = convert_to_serializable(response)
        update_file_result_json(file_hash, json.dumps(response_serializable, ensure_ascii=False))
        return jsonify(response_serializable), 200

    except Exception as e:
        logger.exception("Error applying corrections")
        return jsonify({"error": str(e)}), 500

@app.route('/preview_headers', methods=['POST'])
def preview_headers():
    data = request.get_json()
    file_hash = data.get("file_hash")
    sheet_name = data.get("sheet_name")
    option = data.get("option")

    if not file_hash or not sheet_name or not option:
        return jsonify({"error": "Missing parameters"}), 400

    if file_hash not in file_bytes_cache:
        return jsonify({"error": "File not found"}), 404

    file_bytes = file_bytes_cache[file_hash]

    if option == "1":
        start_row, header_rows = 0, 1
    elif option == "2":
        start_row, header_rows = 1, 1
    else:  # "12"
        start_row, header_rows = 0, 2

    headers = get_preview_headers(file_bytes, sheet_name, start_row, header_rows)
    if headers and isinstance(headers[0], list):
        flat = [" > ".join(str(p) for p in col if p) for col in headers]
    else:
        flat = [str(h) if h is not None else "" for h in headers]

    return jsonify({"headers": flat}), 200

@app.route('/summary/<file_hash>', methods=['GET'])
def get_summary(file_hash):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT summary FROM files WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    conn.close()
    if row and row["summary"]:
        return jsonify({"file_hash": file_hash, "summary": row["summary"]}), 200
    else:
        try:
            summary = summarize_file(file_hash, save=True)
            return jsonify({"file_hash": file_hash, "summary": summary}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)