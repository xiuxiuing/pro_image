from flask import Flask, render_template, request, jsonify, send_file
from data_mgr import DataManager
import os
import pandas as pd

app = Flask(__name__)
base_dir = os.path.dirname(os.path.abspath(__file__))
dm = DataManager(base_dir)

@app.route('/')
def index():
    return render_template('index.html')

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
        "new_orig": safe_f(dm.grid_df.at[row_idx, '新售价'])
    })

@app.route('/api/manual_link', methods=['POST'])
def manual_link():
    data = request.json
    row_idx = data.get('row_idx')
    store_id = data.get('store_id')
    product_data = data.get('product_data')
    dm.manual_link(row_idx, store_id, product_data)
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
