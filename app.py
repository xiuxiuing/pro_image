from flask import Flask, render_template, request, jsonify, send_file
from data_mgr import DataManager
from license_utils import LicenseManager
import os
import pandas as pd
import time
import traceback

import extract_info_ai2
import main_030822

app = Flask(__name__)
base_dir = os.path.dirname(os.path.abspath(__file__))
dm = DataManager(base_dir)

# --- License Check Logic ---
LICENSE_FILE = os.path.join(base_dir, "license.dat")
CURRENT_HWID = LicenseManager.get_hwid()

def check_license():
    if not os.path.exists(LICENSE_FILE):
        return False, "License file missing"
    with open(LICENSE_FILE, "r") as f:
        content = f.read().strip()
    return LicenseManager.verify_license(content, CURRENT_HWID)

@app.route('/api/license_info')
def get_license_info():
    is_valid, msg = check_license()
    return jsonify({
        "hwid": CURRENT_HWID,
        "is_valid": is_valid,
        "message": msg
    })

@app.route('/')
def index():
    is_valid, _ = check_license()
    if not is_valid:
        return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('index.html')

@app.route('/api/run_pipeline', methods=['POST'])
def run_pipeline():
    if 'main_file' not in request.files or 'comp_files' not in request.files:
        return jsonify({"status": "error", "message": "Missing files"}), 400
    
    main_file = request.files['main_file']
    comp_files = request.files.getlist('comp_files')
    
    # 1. Save uploaded files
    upload_dir = os.path.join(base_dir, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    
    main_path = os.path.join(upload_dir, main_file.filename)
    main_file.save(main_path)
    
    comp_paths = []
    for f in comp_files:
        path = os.path.join(upload_dir, f.filename)
        f.save(path)
        comp_paths.append(path)
    
    try:
        # 2. Extract Info AI
        skip_extract = request.form.get('skip_extract') == 'true'
        api_key = request.form.get('api_key')
        
        if not skip_extract:
            if not api_key:
                return jsonify({"status": "error", "message": "Missing API Key"}), 400
            print("Step 1: AI Extraction...")
            extract_info_ai2.process_file_ai(main_path, api_key=api_key, batch_size=110)
            for path in comp_paths:
                extract_info_ai2.process_file_ai(path, api_key=api_key, batch_size=110)
        else:
            print("Step 1: AI Extraction Skipped.")
        
        # 3. Run Analysis
        print("Step 2: Comparison Analysis...")
        output_name = time.strftime("%Y%m%d_%H%M%S")
        output_file = main_030822.run_analysis(main_path, comp_paths, output_name=output_name)
        
        # 4. Update DataManager
        dm.output_file = os.path.join(base_dir, output_file)
        dm.target_file = main_path
        dm.main_store_name = os.path.basename(main_path).replace(".xlsx", "")
        dm.source_files = comp_paths
        dm.store_names = [os.path.basename(p).replace(".xlsx", "") for p in comp_paths]
        dm.load_data()
        
        return jsonify({"status": "success", "output_file": output_file})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/config', methods=['GET', 'POST'])
def get_config():
    if request.method == 'POST':
        data = request.json
        target = data.get('target')
        sources = data.get('sources')
        output = data.get('output')
        
        dm.update_config(target_file=target, source_files=sources, output_file=output)
        return jsonify({"status": "success"})
    
    return jsonify({
        "main_store": dm.main_store_name,
        "target_file": dm.target_file,
        "output_file": dm.output_file,
        "source_files": dm.source_files,
        "stores": [{"id": str(i), "name": name, "path": dm.source_files[i]} for i, name in enumerate(dm.store_names)]
    })

@app.route('/api/list_files')
def list_files():
    files = []
    # Search in root and uploads
    search_dirs = [base_dir, os.path.join(base_dir, "uploads")]
    for d in search_dirs:
        if not os.path.exists(d): continue
        for f in os.listdir(d):
            if f.endswith('.xlsx') or f.endswith('.xls'):
                files.append({
                    "name": f,
                    "path": os.path.join(d, f)
                })
    return jsonify(files)

@app.route('/api/grid_data')
def get_grid_data():
    data = dm.get_grid_data()
    print(f"Serving grid data: {len(data)} rows")
    return jsonify(data)

@app.route('/api/store_products/<store_id>')
def get_store_products(store_id):
    return jsonify(dm.get_store_products(store_id))

@app.route('/api/eliminate', methods=['POST'])
def eliminate():
    data = request.json
    row_idx = data.get('row_idx')
    status = data.get('status', 1)
    dm.eliminate_product(row_idx, status)
    return jsonify({"status": "success"})

@app.route('/api/toggle_add', methods=['POST'])
def toggle_add():
    data = request.json
    row_idx = data.get('row_idx')
    store_id = data.get('store_id')
    is_new = data.get('is_new', True)
    dm.mark_as_new(row_idx, store_id, is_new)
    return jsonify({"status": "success"})

@app.route('/api/price_match', methods=['POST'])
def price_match():
    data = request.json
    row_idx = data.get('row_idx')
    store_id = data.get('store_id')
    dm.price_match(row_idx, store_id)
    
    def safe_f(v):
        if pd.isna(v) or v == "": return ""
        try: return float(v)
        except: return str(v)

    return jsonify({
        "status": "success", 
        "new_act": safe_f(dm.grid_df.at[row_idx, '新活动价']),
        "new_orig": safe_f(dm.grid_df.at[row_idx, '新售价']),
        "store_name": dm.grid_df.at[row_idx, '跟价店']
    })

@app.route('/api/manual_link', methods=['POST'])
def manual_link():
    data = request.json
    row_idx = data.get('row_idx')
    store_id = data.get('store_id')
    product_data = data.get('product_data')
    dm.manual_link(row_idx, store_id, product_data)
    return jsonify({"status": "success"})

@app.route('/api/unlink', methods=['POST'])
def unlink():
    data = request.json
    row_idx = data.get('row_idx')
    store_id = data.get('store_id')
    dm.unlink_product(row_idx, store_id)
    return jsonify({"status": "success"})

@app.route('/api/update_cell', methods=['POST'])
def update_cell():
    data = request.json
    row_idx = data.get('row_idx')
    column = data.get('column')
    value = data.get('value')
    dm.update_cell(row_idx, {column: value})
    return jsonify({"status": "success"})

@app.route('/img/<path:filename>')
def serve_img(filename):
    img_path = os.path.join(base_dir, "img", filename)
    if os.path.exists(img_path):
        return send_file(img_path)
    else:
        return "Image not found", 404

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400
    
    upload_type = request.form.get('type', 'output') # target, source, output
    
    upload_dir = os.path.join(base_dir, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    save_path = os.path.join(upload_dir, file.filename)
    file.save(save_path)
    
    if upload_type == 'target':
        dm.update_config(target_file=save_path)
    elif upload_type == 'output':
        dm.update_config(output_file=save_path)
    # Note: 'source' uploads just save the file, user should select it in config
    
    return jsonify({"status": "success", "path": save_path})

@app.route('/api/export')
def export_data():
    path = dm.save_separate_exports()
    response = send_file(path, as_attachment=True)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route('/api/export_new')
def export_new_data():
    path = dm.export_new_items()
    response = send_file(path, as_attachment=True)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

if __name__ == '__main__':
    # Disable reloader to prevent app restart during background file saves
    app.run(debug=True, port=5001, use_reloader=False)
