import os
import time
import zipfile
import shutil
import tempfile
import pandas as pd
import utils
from data_mgr_base import INTERNAL_EXPORT_KEYS

class DataManagerExportMixin:
    def save_to_excel(self):
        filename = f"对比分析全量成果_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        dirs = self._get_project_dirs(self.active_project_id)
        path = os.path.join(dirs["outputs"], filename)
        
        export_df = self.grid_df.copy()
        cols_to_drop = [c for c in INTERNAL_EXPORT_KEYS if c in export_df.columns]
        if cols_to_drop: export_df.drop(columns=cols_to_drop, inplace=True)
        utils.write_dict_list_to_excel(export_df.fillna("").to_dict(orient='records'), path)
        return path

    def save_separate_exports(self):
        temp_dir = tempfile.mkdtemp()
        try:
            for col in ['新活动价', '新售价', '跟价店', '是否淘汰']:
                if col not in self.grid_df.columns: self.grid_df[col] = ""
            self._calculate_margins()
            
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                new_col, summ_col = f"{prefix}是否新增", f"{store_name}新增"
                self.grid_df[summ_col] = self.grid_df[new_col] if new_col in self.grid_df.columns else ""

            # 1. Main store export
            store_id_to_name = {str(i): name for i, name in enumerate(self.store_names)}
            main_cols = [c for c in self.grid_df.columns if (not c or not c[0].isdigit()) and c not in INTERNAL_EXPORT_KEYS]
            main_export = self.grid_df[main_cols].copy()
            ref_name_col = self.grid_df['ref_name_store'].fillna('') if 'ref_name_store' in self.grid_df.columns else pd.Series('', index=self.grid_df.index)
            ref_image_col = self.grid_df['ref_image_store'].fillna('') if 'ref_image_store' in self.grid_df.columns else pd.Series('', index=self.grid_df.index)

            main_export['名称参考店铺'] = ref_name_col.map(lambda v: store_id_to_name.get(str(v), '') if v else '')
            main_export['参考商品名称'] = [
                self.grid_df.at[i, str(v) + '商品名称'] if v and (str(v) + '商品名称') in self.grid_df.columns else ''
                for i, v in ref_name_col.items()
            ]
            main_export['图片参考店铺'] = ref_image_col.map(lambda v: store_id_to_name.get(str(v), '') if v else '')
            main_export['参考图片链接'] = [
                self.grid_df.at[i, str(v) + '主图链接'] if v and (str(v) + '主图链接') in self.grid_df.columns else ''
                for i, v in ref_image_col.items()
            ]
            main_export.to_excel(os.path.join(temp_dir, f"主店_{self.main_store_name}.xlsx"), index=False)

            # 2. Linked competitor store exports
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                comp_cols = [c for c in self.grid_df.columns if c.startswith(prefix)]
                if not comp_cols: continue
                comp_df = self.grid_df[comp_cols].copy()
                if 'skuId' in self.grid_df.columns: comp_df.insert(0, "主店SKU", self.grid_df['skuId'])
                comp_df.columns = [c if c == "主店SKU" else c[len(prefix):] for c in comp_df.columns]
                comp_df.loc[:, ~comp_df.columns.duplicated()].to_excel(os.path.join(temp_dir, f"竞店_{store_name}.xlsx"), index=False)

            # 3. Unlinked pool export
            unlinked_data = []
            with self._db_lock, self._get_conn() as conn:
                for i, store_name in list(enumerate(self.store_names)):
                    prefix = str(i)
                    query = """
                        SELECT * FROM comp_products 
                        WHERE project_id = ? AND store_id = ?
                        AND skuId NOT IN (
                            SELECT comp_sku_id FROM product_links 
                            WHERE project_id = ? AND store_id = ?
                        )
                    """
                    df = pd.read_sql(query, conn, params=(self.active_project_id, prefix, self.active_project_id, prefix))
                    if not df.empty:
                        df.insert(0, "竞品店铺", store_name)
                        unlinked_data.append(df)
            
                if unlinked_data:
                    pool_df = pd.concat(unlinked_data, ignore_index=True)
                    pool_df.to_excel(os.path.join(temp_dir, "未关联商品池.xlsx"), index=False)
            
            dirs = self._get_project_dirs(self.active_project_id)
            zip_path = os.path.join(dirs["outputs"], f"对比成果_{time.strftime('%Y%m%d_%H%M%S')}.zip")
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for root, _, files in os.walk(temp_dir):
                    for file in files: zf.write(os.path.join(root, file), arcname=file)
            return zip_path
        finally: shutil.rmtree(temp_dir)

    def export_new_items(self):
        op_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self._calculate_margins()
        
        # Mapping prefix to store name
        store_map = {str(i): name for i, name in enumerate(self.store_names)}
        
        # 1. Fetch ALL products marked as "New" from comp_products (includes linked + unlinked)
        all_new_data = []
        with self._db_lock, self._get_conn() as conn:
            query = "SELECT * FROM comp_products WHERE project_id = ? AND is_new_add = '是'"
            all_comp_new_df = pd.read_sql(query, conn, params=(self.active_project_id,))
            if not all_comp_new_df.empty:
                # Add store name and main store link info if available
                all_comp_new_df['竞品店铺'] = all_comp_new_df['store_id'].map(store_map)
                
                # Fetch links to get Main Store SKU if linked
                link_query = "SELECT comp_sku_id, main_sku_id, store_id FROM product_links WHERE project_id = ?"
                links = pd.read_sql(link_query, conn, params=(self.active_project_id,))
                
                # Merge to get Main SKU (store-aware to avoid cross-store false joins)
                merged = all_comp_new_df.merge(
                    links,
                    left_on=['skuId', 'store_id'],
                    right_on=['comp_sku_id', 'store_id'],
                    how='left'
                )
                merged.rename(columns={'main_sku_id': '主店SKU'}, inplace=True)
                merged['来源'] = merged['主店SKU'].apply(lambda v: '已匹配' if str(v).strip() not in ['', 'nan', 'None'] else '未匹配池')
                
                # Ensure core columns
                cols = ['来源', '主店SKU', '竞品店铺', 'skuId', '主图链接', '商品名称', '规格名称', '活动价', '原价', '销售', '条码']
                final_new_df = merged[[c for c in cols if c in merged.columns]].copy()
                for c in cols:
                    if c not in final_new_df.columns: final_new_df[c] = ""
                all_new_data = final_new_df.fillna("").to_dict(orient='records')

        final_df = pd.DataFrame(all_new_data) if all_new_data else pd.DataFrame(columns=["主店SKU", "竞品店铺", "skuId", "主图链接", "商品名称", "规格名称", "活动价", "原价", "销售", "条码"])
        final_df["操作时间"] = op_time

        # 2. Main store: eliminated / price-matched / ref-marked items
        mask = pd.Series(False, index=self.grid_df.index)
        if '是否淘汰' in self.grid_df.columns: mask |= (self.grid_df['是否淘汰'] == "是")
        if '跟价店' in self.grid_df.columns: mask |= (self.grid_df['跟价店'].notna() & (self.grid_df['跟价店'] != ""))
        if 'ref_name_store' in self.grid_df.columns: mask |= (self.grid_df['ref_name_store'].fillna('') != '')
        if 'ref_image_store' in self.grid_df.columns: mask |= (self.grid_df['ref_image_store'].fillna('') != '')
            
        elim_df = self.grid_df[mask].copy()
        if not elim_df.empty:
            main_cols = [c for c in elim_df.columns if (not c or not c[0].isdigit()) and c not in INTERNAL_EXPORT_KEYS]
            elim_export = elim_df[main_cols].copy()
            ref_name_s = elim_df['ref_name_store'].fillna('') if 'ref_name_store' in elim_df.columns else pd.Series('', index=elim_df.index)
            ref_image_s = elim_df['ref_image_store'].fillna('') if 'ref_image_store' in elim_df.columns else pd.Series('', index=elim_df.index)
            elim_export['名称参考店铺'] = ref_name_s.map(lambda v: store_map.get(str(v), '') if v else '')
            elim_export['参考商品名称'] = [
                elim_df.at[i, str(v) + '商品名称'] if v and (str(v) + '商品名称') in elim_df.columns else ''
                for i, v in ref_name_s.items()
            ]
            elim_export['图片参考店铺'] = ref_image_s.map(lambda v: store_map.get(str(v), '') if v else '')
            elim_export['参考图片链接'] = [
                elim_df.at[i, str(v) + '主图链接'] if v and (str(v) + '主图链接') in elim_df.columns else ''
                for i, v in ref_image_s.items()
            ]
            elim_export["操作时间"] = op_time
            elim_df = elim_export
        else: elim_df = pd.DataFrame(columns=["skuId", "主图链接", "商品名称", "规格名称", "活动价", "原价", "销售", "条码", "操作时间"])

        dirs = self._get_project_dirs(self.active_project_id)
        path = os.path.join(dirs["outputs"], f"新增竞品数据_{time.strftime('%Y%m%d_%H%M%S')}.xlsx")
        sheet_data = {"新增(竞店)": final_df.fillna("").to_dict(orient='records'), "淘汰(主店)": elim_df.fillna("").to_dict(orient='records')}
        utils.write_multisheet_dict_to_excel(sheet_data, path); return path
