from flask import Flask, render_template, request, jsonify, send_file
from data_mgr import DataManager
import os
import pandas as pd
import time
import traceback

import extract_info_ai2
import main_030822

app = Flask(__name__)
base_dir = os.path.dirname(os.path.abspath(__file__))
dm = DataManager(base_dir)

@app.route('/')
def index():
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
        print("Step 1: AI Extraction...")
        extract_info_ai2.process_file_ai(main_path, batch_size=110)
        for path in comp_paths:
            extract_info_ai2.process_file_ai(path, batch_size=110)
        
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

@app.route('/api/config')
def get_config():
    return jsonify({
        "main_store": dm.main_store_name,
        "stores": [{"id": str(i), "name": name} for i, name in enumerate(dm.store_names)]
    })

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

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400
    
    # Save to the local output file path
    save_path = dm.output_file
    file.save(save_path)
    
    # Force reload
    dm.load_data()
    return jsonify({"status": "success"})

@app.route('/api/export')
def export_data():
    path = dm.save_separate_exports()
    return send_file(path, as_attachment=True, mimetype='application/zip')

if __name__ == '__main__':
    # Using a dynamic port to avoid conflicts
    app.run(debug=True, port=5001)
