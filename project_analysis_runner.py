import os
import shutil
import time
import traceback

import quality_preflight


class ProgressFns:
    def __init__(self, init_progress, init_import_progress, update_step, schedule_clear_progress):
        self.init_progress = init_progress
        self.init_import_progress = init_import_progress
        self.update_step = update_step
        self.schedule_clear_progress = schedule_clear_progress


def _filtered_source_files(dm, payload):
    import pandas as pd
    import utils
    from data_mgr_base import FIELD_MAPPINGS

    dirs = payload["dirs"]
    main_path = payload["main_path"]
    comp_paths = list(payload.get("comp_paths") or [])
    column_mappings = payload.get("column_mappings") or {}
    partial_categories = list(payload.get("partial_categories") or [])

    if hasattr(dm, "storage"):
        cache_dir = dm.storage.cache_dir(payload["project_id"], f"analysis_input_{int(time.time())}")
    else:
        cache_dir = os.path.join(dirs["cache"], f"analysis_input_{int(time.time())}")
        os.makedirs(cache_dir, exist_ok=True)

    norm_main = quality_preflight.normalize_file_for_analysis(
        main_path,
        os.path.join(cache_dir, "main_normalized.xlsx"),
        (column_mappings or {}).get("main") or {},
    )
    norm_comps = [
        quality_preflight.normalize_file_for_analysis(
            p,
            os.path.join(cache_dir, f"comp_{idx}_normalized.xlsx"),
            (column_mappings or {}).get(f"comp_{idx}") or {},
        )
        for idx, p in enumerate(comp_paths)
    ]
    if not partial_categories:
        return norm_main, norm_comps

    selected = set(partial_categories)

    def _filter_file(path, name):
        rows = utils.excel_to_list_dict(path)
        df = pd.DataFrame(rows)
        if df.empty:
            out = os.path.join(cache_dir, name)
            df.to_excel(out, index=False)
            return out
        df = dm._apply_mappings(df, FIELD_MAPPINGS)
        if "美团类目三级" not in df.columns:
            df = df.iloc[0:0].copy()
        else:
            cat = df["美团类目三级"].fillna("").map(utils.clean_text_value).astype(str).str.strip()
            df = df[cat.isin(selected)].copy()
        out = os.path.join(cache_dir, name)
        df.to_excel(out, index=False)
        return out

    filtered_main = _filter_file(norm_main, "main_partial.xlsx")
    filtered_comps = [_filter_file(p, f"comp_{idx}_partial.xlsx") for idx, p in enumerate(norm_comps)]
    return filtered_main, filtered_comps


def run_manual_import(dm, payload, progress: ProgressFns):
    pid = int(payload["project_id"])
    output_file = payload["output_file"]
    partial_cats = list(payload.get("partial_categories") or [])
    import_labels = ["保存关联文件", "解析关联结果", "写入关联数据", "完成"]
    try:
        if payload.get("job_id"):
            progress.init_import_progress(pid, import_labels, payload.get("job_id"))
        else:
            progress.init_import_progress(pid, import_labels)
        progress.update_step(pid, 0, "done")
        progress.update_step(pid, 1, "running")
        links_df = dm.parse_links_from_output(pid, output_file)
        progress.update_step(pid, 1, "done")
        progress.update_step(pid, 2, "running")
        if partial_cats:
            dm.replace_project_links(pid, links_df, categories=partial_cats)
        else:
            dm.replace_project_links(pid, links_df)
        progress.update_step(pid, 2, "done")
        progress.update_step(pid, 3, "running")
        dm.update_project_status(pid, "ready")
        progress.update_step(pid, 3, "done")
    except Exception:
        traceback.print_exc()
        try:
            dm.update_project_status(pid, "failed")
        except Exception:
            pass
    finally:
        progress.schedule_clear_progress(pid)


def run_auto_analysis(dm, payload, progress: ProgressFns):
    import extract_info_ai2
    import main_030822

    pid = int(payload["project_id"])
    dirs = payload["dirs"]
    comp_names = list(payload.get("comp_names") or [])
    main_name = payload.get("main_name") or ""
    partial_categories = list(payload.get("partial_categories") or [])
    match_config_json = payload.get("match_config_json") or ""
    use_ai = bool(payload.get("use_ai"))
    api_key = (payload.get("api_key") or "").strip()
    has_ai = bool(use_ai and api_key)
    if payload.get("job_id"):
        prog = progress.init_progress(pid, has_ai, main_name, comp_names, payload.get("job_id"))
    else:
        prog = progress.init_progress(pid, has_ai, main_name, comp_names)
    ai_file_count = (1 + len(comp_names)) if has_ai else 0
    try:
        analysis_main_path, analysis_comp_paths = _filtered_source_files(dm, payload)
        if has_ai:
            all_ai_paths = [analysis_main_path] + analysis_comp_paths
            ai_gap = int(os.environ.get("PROIMAGE_AI_INTER_FILE_SLEEP_SEC", "8") or "8")
            for fi, fp in enumerate(all_ai_paths):
                progress.update_step(pid, fi, "running")

                def _ai_cb(batch, total, _fi=fi):
                    progress.update_step(pid, _fi, "running", f"batch {batch}/{total}")

                extract_info_ai2.process_file_ai(
                    fp,
                    api_key,
                    progress_cb=_ai_cb,
                    model_name=payload.get("ai_model_name") or "",
                    fallback_api_key=payload.get("kimi_api_key") or None,
                    fallback_model=payload.get("kimi_model_name") or None,
                    provider=payload.get("ai_provider") or "",
                    fallback_provider=payload.get("fallback_provider") or "",
                )
                progress.update_step(pid, fi, "done")
                if fi + 1 < len(all_ai_paths) and ai_gap > 0:
                    time.sleep(ai_gap)

        analysis_base = ai_file_count

        def _analysis_cb(event, idx=0, detail=""):
            if event == "source_start":
                progress.update_step(pid, analysis_base + idx, "running", detail)
            elif event == "source_done":
                progress.update_step(pid, analysis_base + idx, "done")
            elif event == "query_start":
                progress.update_step(pid, len(prog["steps"]) - 1, "running", detail)
            elif event == "query_progress":
                progress.update_step(pid, len(prog["steps"]) - 1, "running", detail)

        post_match_template = dm.get_post_match_template_for_project(pid)
        run_stamp = int(time.time())
        output_name = f"partial_{pid}_{run_stamp}" if partial_categories else f"{pid}_{run_stamp}"
        analysis_metrics = {}
        main_030822.run_analysis(
            analysis_main_path,
            analysis_comp_paths,
            output_name=output_name,
            output_dir=dirs["outputs"],
            progress_cb=_analysis_cb,
            match_config=match_config_json,
            post_match_template=post_match_template,
            analysis_metrics=analysis_metrics,
        )
        progress.update_step(pid, len(prog["steps"]) - 1, "done", "分析完成")
        report = quality_preflight.build_quality_report(
            payload.get("preflight") or {},
            analysis_metrics,
            {"project_id": pid, "output_name": output_name, "partial_categories": partial_categories},
        )
        report_path = os.path.join(dirs["outputs"], f"quality_report_{output_name}.json")
        quality_preflight.save_quality_report(report, report_path)
        quality_preflight.save_quality_report(report, os.path.join(dirs["outputs"], "quality_report_latest.json"))
        if partial_categories:
            partial_output = os.path.join(dirs["outputs"], f"output_{output_name}.xlsx")
            links_df = dm.parse_links_from_output(pid, partial_output)
            dm.replace_project_links(pid, links_df, categories=partial_categories)
        else:
            full_output = os.path.join(dirs["outputs"], f"output_{output_name}.xlsx")
            links_df = dm.parse_links_from_output(pid, full_output)
            dm.replace_project_links(pid, links_df)
            try:
                shutil.copy2(full_output, os.path.join(dirs["outputs"], f"output_{pid}.xlsx"))
            except Exception:
                traceback.print_exc()
        dm.update_project_status(pid, "ready")
        try:
            dm.activate_project(pid, skip_load=True)
        except Exception:
            traceback.print_exc()
    except BaseException:
        traceback.print_exc()
        try:
            dm.update_project_status(pid, "failed")
        except Exception:
            pass
    finally:
        progress.schedule_clear_progress(pid)
