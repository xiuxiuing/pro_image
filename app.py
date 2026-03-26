from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from data_mgr import DataManager
from license_utils import LicenseManager
import os
import shutil
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
    if not os.path.exists(LICENSE_FILE): return False, "License file missing"
    with open(LICENSE_FILE, "r") as f: content = f.read().strip()
    return LicenseManager.verify_license(content, CURRENT_HWID)

@app.route('/api/license_info')
def get_license_info():
    is_valid, msg = check_license()
    return jsonify({"hwid": CURRENT_HWID, "is_valid": is_valid, "message": msg})

@app.route('/')
def projects_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('projects.html')

@app.route('/dashboard')
def index():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('index.html', active_project=dm.active_project_name)

@app.route('/api/projects', methods=['GET', 'POST'])
def handle_projects():
    if request.method == 'POST':
        name = request.form.get('name')
        if not name: return jsonify({"status": "error", "message": "Project name is required"}), 400
        
        main_file = request.files.get('main_file')
        comp_files = request.files.getlist('comp_files')
        
        if not main_file or not main_file.filename:
            return jsonify({"status": "error", "message": "Main store file is required"}), 400
        
        valid_comp_files = [f for f in comp_files if f.filename]
        if not valid_comp_files:
            return jsonify({"status": "error", "message": "At least one competitor store file is required"}), 400
        
        # Temporary PID for directory naming
        temp_pid = int(time.time())
        proj_dir = os.path.join(base_dir, "uploads", f"project_{temp_pid}")
        os.makedirs(proj_dir, exist_ok=True)
        
        # Save Main Store File
        main_path = os.path.join(proj_dir, main_file.filename)
        main_file.save(main_path)
        main_store_name = main_file.filename.replace(".xlsx", "").replace(".xls", "")
        
        # Save Competitor Store Files
        comp_infos = []
        comp_paths = []
        for f in valid_comp_files:
            path = os.path.join(proj_dir, f.filename)
            f.save(path)
            comp_paths.append(path)
            comp_infos.append({"path": path, "store_name": f.filename.replace(".xlsx", "").replace(".xls", "")})
        
        # Handle manual result file if Skip Analysis is selected
        result_file = request.files.get('result_file')
        manual_result_path = None
        if result_file and result_file.filename:
            manual_result_path = os.path.join(proj_dir, result_file.filename)
            result_file.save(manual_result_path)

        # Create project in DB
        pid = dm.create_project(name, {"path": main_path, "store_name": main_store_name}, comp_infos)
        
        # Rename directory to real PID
        real_proj_dir = os.path.join(base_dir, "uploads", f"project_{pid}")
        if os.path.exists(real_proj_dir): shutil.rmtree(real_proj_dir)
        os.rename(proj_dir, real_proj_dir)
        
        # Update paths in DB
        with dm._db_lock:
            with dm._get_conn() as conn:
                conn.execute("UPDATE project_files SET local_path = REPLACE(local_path, ?, ?) WHERE project_id = ?", 
                            (f"project_{temp_pid}", f"project_{pid}", pid))

        # Update internal DataManager state and paths
        dm.activate_project(pid)
        
        # Final paths for analysis or manual setup
        final_main_path = main_path.replace(f"project_{temp_pid}", f"project_{pid}")
        final_comp_paths = [p.replace(f"project_{temp_pid}", f"project_{pid}") for p in comp_paths]
        final_manual_result_path = manual_result_path.replace(f"project_{temp_pid}", f"project_{pid}") if manual_result_path else None

        use_ai = request.form.get('use_ai') == 'on'
        api_key = request.form.get('api_key')

        # Initial analysis flow
        try:
            if final_manual_result_path:
                # IMPORTANT: Even when skipping analysis, we need the output file in place
                # The dm.output_file is set during dm.activate_project, which creates a default output path.
                # We copy the manual result file to this default output path.
                shutil.copy(final_manual_result_path, dm.output_file)
                dm.load_data() # Load the data from the copied manual result file
            else:
                # Optional AI Extraction
                if use_ai and api_key:
                    print(f"Starting AI extraction with API Key: {api_key[:5]}***")
                    extract_info_ai2.process_file_ai(final_main_path, api_key)
                    for comp_path in final_comp_paths:
                        extract_info_ai2.process_file_ai(comp_path, api_key)
                
                # Standard AI Analysis
                output_file = main_030822.run_analysis(final_main_path, final_comp_paths, output_name=time.strftime("%y%m%d_%H%M%S"))
                output_path = os.path.join(base_dir, output_file)
                dm.update_config(target_file=final_main_path, source_files=final_comp_paths, output_file=output_path)
        except Exception as e:
            traceback.print_exc()
        
        return jsonify({"status": "success", "project_id": pid})
        
    return jsonify(dm.list_projects())

@app.route('/api/projects/<int:pid>/activate', methods=['POST'])
def activate_project(pid):
    dm.activate_project(pid)
    return jsonify({"status": "success"})

@app.route('/api/projects/<int:pid>', methods=['DELETE'])
def delete_project(pid):
    dm.delete_project(pid)
    return jsonify({"status": "success"})

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        "main_store": dm.main_store_name, "target_file": dm.target_file, "output_file": dm.output_file,
        "source_files": dm.source_files, "stores": [{"id": str(i), "name": n, "path": dm.source_files[i]} for i, n in enumerate(dm.store_names)]
    })

@app.route('/api/grid_data')
def get_grid_data():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    mode = request.args.get('mode', "all")
    return jsonify(dm.get_paginated_grid(page=page, limit=limit, search=search, mode=mode))

@app.route('/api/store_products/<store_id>')
def get_store_products(store_id):
    return jsonify(dm.get_store_products(store_id))


@app.route('/api/unlinked_items')
def get_unlinked_items():
    return jsonify(dm.get_unlinked_products())

@app.route('/api/eliminate', methods=['POST'])
def eliminate():
    d = request.json
    dm.eliminate_product(d.get('row_idx'), d.get('status', 1))
    return jsonify({"status": "success"})

@app.route('/api/toggle_add', methods=['POST'])
@app.route('/api/toggle_add', methods=['POST'])
def toggle_add():
    d = request.json
    dm.mark_as_new(d.get('row_idx'), d.get('store_id'), d.get('is_new', True), sku_id=d.get('sku_id'))
    return jsonify({"status": "success"})

@app.route('/api/price_match', methods=['POST'])
def price_match():
    d = request.json
    row_idx, store_id = d.get('row_idx'), d.get('store_id')
    dm.price_match(row_idx, store_id)
    
    def f(v):
        if pd.isna(v) or v == "": return ""
        try: return float(v)
        except: return str(v)

    return jsonify({
        "status": "success", "new_act": f(dm.grid_df.at[row_idx, '新活动价']),
        "new_orig": f(dm.grid_df.at[row_idx, '新售价']), "store_name": dm.grid_df.at[row_idx, '跟价店']
    })

@app.route('/api/manual_link', methods=['POST'])
def manual_link():
    d = request.json
    dm.manual_link(d.get('row_idx'), d.get('store_id'), d.get('product_data'))
    return jsonify({"status": "success"})

@app.route('/api/unlink', methods=['POST'])
def unlink():
    d = request.json
    dm.unlink_product(d.get('row_idx'), d.get('store_id'))
    return jsonify({"status": "success"})

@app.route('/api/update_cell', methods=['POST'])
def update_cell():
    d = request.json
    dm.update_cell(d.get('row_idx'), {d.get('column'): d.get('value')})
    return jsonify({"status": "success"})

@app.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(base_dir, "img"), filename)


@app.route('/api/export')
def export_data():
    p = dm.save_separate_exports()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@app.route('/api/export_new')
def export_new_data():
    p = dm.export_new_items()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

if __name__ == '__main__':
    app.run(debug=False, port=5001)
