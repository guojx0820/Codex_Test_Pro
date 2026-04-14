#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import queue
import threading
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

from download import DownloadEngine, DownloadTask

DATASETS = [
    "sentinel-2-l2a",
    "landsat-c2-l2",
    "sentinel-1-grd",
    "modis-13q1-061",
    "modis-09a1-061",
    "goci-l2",
]
HISTORY_FILE = "task_history.json"


class RemoteSensingDownloaderApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("遥感数据批量下载工具 v1.4")
        self.root.geometry("1180x820")

        self.log_queue: "queue.Queue[str]" = queue.Queue()
        self.is_running = False
        self.engine = DownloadEngine(self.log)

        self.dataset_vars: dict[str, tk.BooleanVar] = {}
        self.start_date_var = tk.StringVar(value="2025-01-01")
        self.end_date_var = tk.StringVar(value="2025-01-31")
        self.aoi_mode_var = tk.StringVar(value="BBox")
        self.bbox_var = tk.StringVar(value="116.0,39.6,116.8,40.2")
        self.vector_path_var = tk.StringVar(value="")

        self.max_items_var = tk.StringVar(value="10")
        self.asset_limit_var = tk.StringVar(value="2")
        self.cloud_cover_var = tk.StringVar(value="30")
        self.retry_var = tk.StringVar(value="2")
        self.workers_var = tk.StringVar(value="4")
        self.resume_var = tk.BooleanVar(value=True)

        self.nasa_user_var = tk.StringVar(value="")
        self.nasa_password_var = tk.StringVar(value="")
        self.nasa_token_var = tk.StringVar(value="")

        self.output_dir_var = tk.StringVar(value=os.path.join(os.getcwd(), "downloads"))
        self.history_path = Path(os.getcwd()) / HISTORY_FILE

        self._build_ui()
        self._poll_log_queue()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        dataset_group = ttk.LabelFrame(frame, text="1) 选择数据集（可多选）", padding=10)
        dataset_group.pack(fill=tk.X, pady=6)
        cols = 3
        for idx, ds in enumerate(DATASETS):
            v = tk.BooleanVar(value=(idx < 3))
            self.dataset_vars[ds] = v
            ttk.Checkbutton(dataset_group, text=ds, variable=v).grid(row=idx // cols, column=idx % cols, sticky=tk.W, padx=8, pady=4)

        filter_group = ttk.LabelFrame(frame, text="2) 检索与下载设置", padding=10)
        filter_group.pack(fill=tk.X, pady=6)

        ttk.Label(filter_group, text="开始日期").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.start_date_var, width=16).grid(row=0, column=1, padx=6)
        ttk.Label(filter_group, text="结束日期").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.end_date_var, width=16).grid(row=0, column=3, padx=6)

        ttk.Label(filter_group, text="空间输入").grid(row=1, column=0, sticky=tk.W)
        ttk.Combobox(filter_group, textvariable=self.aoi_mode_var, values=["BBox", "GeoJSON", "Shapefile"], state="readonly", width=12).grid(row=1, column=1, sticky=tk.W)
        ttk.Label(filter_group, text="BBox").grid(row=2, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.bbox_var, width=58).grid(row=2, column=1, columnspan=3, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="GeoJSON/Shapefile").grid(row=3, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.vector_path_var, width=72).grid(row=3, column=1, columnspan=2, sticky=tk.W, padx=6)
        ttk.Button(filter_group, text="浏览", command=self.choose_vector_file).grid(row=3, column=3, sticky=tk.W)

        ttk.Label(filter_group, text="每数据集景数").grid(row=4, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.max_items_var, width=16).grid(row=4, column=1, sticky=tk.W)
        ttk.Label(filter_group, text="每景资产数").grid(row=4, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.asset_limit_var, width=16).grid(row=4, column=3, sticky=tk.W)

        ttk.Label(filter_group, text="最大云量%").grid(row=5, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.cloud_cover_var, width=16).grid(row=5, column=1, sticky=tk.W)
        ttk.Label(filter_group, text="失败重试").grid(row=5, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.retry_var, width=16).grid(row=5, column=3, sticky=tk.W)

        ttk.Label(filter_group, text="并发线程").grid(row=6, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.workers_var, width=16).grid(row=6, column=1, sticky=tk.W)
        ttk.Checkbutton(filter_group, text="断点续传", variable=self.resume_var).grid(row=6, column=2, sticky=tk.W)

        auth_group = ttk.LabelFrame(frame, text="3) NASA Earthdata（MODIS建议填写）", padding=10)
        auth_group.pack(fill=tk.X, pady=6)
        ttk.Label(auth_group, text="NASA用户名").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(auth_group, textvariable=self.nasa_user_var, width=24).grid(row=0, column=1, padx=6)
        ttk.Label(auth_group, text="NASA密码").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(auth_group, textvariable=self.nasa_password_var, width=24, show="*").grid(row=0, column=3, padx=6)
        ttk.Label(auth_group, text="NASA Token").grid(row=0, column=4, sticky=tk.W)
        ttk.Entry(auth_group, textvariable=self.nasa_token_var, width=36).grid(row=0, column=5, padx=6)

        out_group = ttk.LabelFrame(frame, text="4) 输出目录", padding=10)
        out_group.pack(fill=tk.X, pady=6)
        ttk.Entry(out_group, textvariable=self.output_dir_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(out_group, text="浏览", command=self.choose_output_dir).pack(side=tk.LEFT, padx=6)

        action = ttk.Frame(frame)
        action.pack(fill=tk.X, pady=6)
        self.start_button = ttk.Button(action, text="开始批量下载", command=self.start_download)
        self.start_button.pack(side=tk.LEFT)
        ttk.Button(action, text="下载前链路预检", command=self.precheck_download_links).pack(side=tk.LEFT, padx=8)
        ttk.Button(action, text="查看历史任务", command=self.show_history).pack(side=tk.LEFT, padx=8)
        self.progress = ttk.Progressbar(action, mode="indeterminate")
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)

        log_group = ttk.LabelFrame(frame, text="运行日志", padding=10)
        log_group.pack(fill=tk.BOTH, expand=True, pady=6)
        self.log_text = tk.Text(log_group, height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def choose_output_dir(self) -> None:
        d = filedialog.askdirectory(title="选择输出目录")
        if d:
            self.output_dir_var.set(d)

    def choose_vector_file(self) -> None:
        f = filedialog.askopenfilename(title="选择矢量文件", filetypes=[("Vector", "*.geojson *.json *.shp"), ("All", "*.*")])
        if f:
            self.vector_path_var.set(f)

    def log(self, message: str) -> None:
        self.log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def _poll_log_queue(self) -> None:
        try:
            while True:
                m = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, m + "\n")
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        finally:
            self.root.after(150, self._poll_log_queue)

    def _load_geometry(self) -> tuple[list[float] | None, dict[str, Any] | None]:
        mode = self.aoi_mode_var.get()
        if mode == "BBox":
            p = [x.strip() for x in self.bbox_var.get().split(",")]
            if len(p) != 4:
                raise ValueError("BBox格式错误")
            return [float(v) for v in p], None

        path = self.vector_path_var.get().strip()
        if not path:
            raise ValueError("请选择矢量文件")

        if mode == "GeoJSON":
            with open(path, "r", encoding="utf-8") as f:
                gj = json.load(f)
            if gj.get("type") == "FeatureCollection":
                return None, gj["features"][0]["geometry"]
            if gj.get("type") == "Feature":
                return None, gj["geometry"]
            return None, gj

        import shapefile  # type: ignore

        reader = shapefile.Reader(path)
        pts = reader.shapes()[0].points
        if pts[0] != pts[-1]:
            pts.append(pts[0])
        return None, {"type": "Polygon", "coordinates": [[list(p) for p in pts]]}

    def _validate(self) -> tuple[list[str], list[float] | None, dict[str, Any] | None, int, int, int, int, int]:
        datasets = [k for k, v in self.dataset_vars.items() if v.get()]
        if not datasets:
            raise ValueError("至少选择一个数据集")
        datetime.strptime(self.start_date_var.get(), "%Y-%m-%d")
        datetime.strptime(self.end_date_var.get(), "%Y-%m-%d")
        bbox, geom = self._load_geometry()
        return (
            datasets,
            bbox,
            geom,
            max(1, int(self.max_items_var.get())),
            max(1, int(self.asset_limit_var.get())),
            int(self.cloud_cover_var.get()),
            max(0, int(self.retry_var.get())),
            min(16, max(1, int(self.workers_var.get()))),
        )


    def precheck_download_links(self) -> None:
        try:
            datasets, bbox, geometry, max_items, asset_limit, cloud, retry, workers = self._validate()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("输入错误", str(exc))
            return

        try:
            session = self.engine.build_session(self.nasa_user_var.get(), self.nasa_password_var.get(), self.nasa_token_var.get())
            self.engine.verify_auth(session)

            all_tasks: list[DownloadTask] = []
            for ds in datasets:
                items = self.engine.search_dataset(
                    session, ds, bbox, geometry, self.start_date_var.get(), self.end_date_var.get(), max_items, cloud
                )
                tasks, filtered = self.engine.build_tasks(ds, items, self.output_dir_var.get(), asset_limit)
                self.log(f"预检-{ds}: 记录{len(items)} 条, 可下载任务{len(tasks)} 个, 过滤{filtered} 个")
                all_tasks.extend(tasks)

            result = self.engine.precheck_tasks(session, all_tasks, max_checks=30)
            msg = (
                f"预检完成\n"
                f"检查任务数: {result['checked']}\n"
                f"可下载: {result['ok']}\n"
                f"需认证/认证失败: {result['auth']}\n"
                f"预计失败: {result['bad']}"
            )
            self.log(msg.replace("\n", " | "))
            messagebox.showinfo("链路预检结果", msg)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("预检失败", str(exc))

    def start_download(self) -> None:
        if self.is_running:
            messagebox.showinfo("提示", "任务运行中")
            return
        try:
            args = self._validate()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("输入错误", str(exc))
            return

        self.is_running = True
        self.start_button.configure(state=tk.DISABLED)
        self.progress.start(10)
        t = threading.Thread(target=self._run_download, args=(*args, self.output_dir_var.get()), daemon=True)
        t.start()

    def _run_download(
        self,
        datasets: list[str],
        bbox: list[float] | None,
        geometry: dict[str, Any] | None,
        max_items: int,
        asset_limit: int,
        cloud: int,
        retry: int,
        workers: int,
        output_dir: str,
    ) -> None:
        ok = 0
        fail = 0
        filtered_total = 0
        history: list[dict[str, Any]] = []

        try:
            os.makedirs(output_dir, exist_ok=True)
            session = self.engine.build_session(self.nasa_user_var.get(), self.nasa_password_var.get(), self.nasa_token_var.get())
            self.engine.verify_auth(session)

            all_tasks: list[DownloadTask] = []
            for ds in datasets:
                self.log(f"开始检索数据集: {ds}")
                items = self.engine.search_dataset(
                    session,
                    ds,
                    bbox,
                    geometry,
                    self.start_date_var.get(),
                    self.end_date_var.get(),
                    max_items,
                    cloud,
                )
                self.log(f"{ds} 检索到 {len(items)} 条记录")
                tasks, filtered = self.engine.build_tasks(ds, items, output_dir, asset_limit)
                filtered_total += filtered
                self.log(f"{ds} 计划下载 {len(tasks)} 个文件，过滤无效资产 {filtered} 个")
                all_tasks.extend(tasks)

            total = len(all_tasks)
            if total == 0:
                self.log("无可下载文件")
            else:
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futures = {ex.submit(self.engine.download_one, session, task, retry, self.resume_var.get()): task for task in all_tasks}
                    for i, fut in enumerate(as_completed(futures), start=1):
                        task = futures[fut]
                        success = False
                        try:
                            success = fut.result()
                        except Exception as exc:  # noqa: BLE001
                            self.log(f"下载线程异常: {exc}")
                        if success:
                            ok += 1
                            self.log(f"[{i}/{total}] 完成: {os.path.basename(task.output_path)} | 类型 {task.file_type}")
                        else:
                            fail += 1
                            self.log(f"[{i}/{total}] 失败: {task.url} | 类型 {task.file_type}")
                        history.append({**asdict(task), "success": success, "time": datetime.now().isoformat()})

            self._save_history(history)
            msg = f"全部完成，成功 {ok}，失败 {fail}，过滤资产 {filtered_total}。"
            self.log(msg)
            messagebox.showinfo("完成", msg)
        except Exception as exc:  # noqa: BLE001
            self.log(f"任务失败: {exc}")
            messagebox.showerror("任务失败", str(exc))
        finally:
            self.root.after(0, self._finish)

    def _finish(self) -> None:
        self.is_running = False
        self.start_button.configure(state=tk.NORMAL)
        self.progress.stop()

    def _save_history(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        old: list[dict[str, Any]] = []
        if self.history_path.exists():
            try:
                old = json.loads(self.history_path.read_text(encoding="utf-8"))
                if not isinstance(old, list):
                    old = []
            except Exception:
                old = []
        old.extend(records)
        self.history_path.write_text(json.dumps(old[-3000:], ensure_ascii=False, indent=2), encoding="utf-8")

    def show_history(self) -> None:
        if not self.history_path.exists():
            messagebox.showinfo("历史", "暂无历史")
            return
        try:
            data = json.loads(self.history_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("历史", f"读取失败: {exc}")
            return
        lines = []
        for r in data[-30:]:
            lines.append(f"{r.get('time','')} | {'成功' if r.get('success') else '失败'} | {r.get('dataset','')} | {r.get('file_type','')}")
        messagebox.showinfo("最近30条", "\n".join(lines) if lines else "暂无")


def main() -> None:
    root = tk.Tk()
    RemoteSensingDownloaderApp(root)
    root.minsize(1080, 760)
    root.mainloop()


if __name__ == "__main__":
    main()
