#!/usr/bin/env python3
"""遥感数据批量下载工具（GUI）。"""

from __future__ import annotations

import json
import os
import queue
import threading
import tkinter as tk
from dataclasses import dataclass
from datetime import datetime
from tkinter import filedialog, messagebox, ttk
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

STAC_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"
DEFAULT_DATASETS = [
    "sentinel-2-l2a",
    "landsat-c2-l2",
    "sentinel-1-grd",
]
DEFAULT_ASSET_PREFERENCE = {
    "sentinel-2-l2a": ["visual", "B04", "B03", "B02"],
    "landsat-c2-l2": ["rendered_preview", "red", "green", "blue", "qa_pixel"],
    "sentinel-1-grd": ["vv", "vh"],
}


@dataclass
class DownloadTask:
    dataset: str
    item_id: str
    asset_key: str
    url: str
    output_path: str


class RemoteSensingDownloaderApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("遥感数据批量下载工具 v1.0")
        self.root.geometry("980x680")

        self.log_queue: "queue.Queue[str]" = queue.Queue()
        self.is_running = False

        self.dataset_vars: dict[str, tk.BooleanVar] = {}
        self.start_date_var = tk.StringVar(value="2025-01-01")
        self.end_date_var = tk.StringVar(value="2025-01-31")
        self.bbox_var = tk.StringVar(value="116.0,39.6,116.8,40.2")
        self.max_items_var = tk.StringVar(value="10")
        self.cloud_cover_var = tk.StringVar(value="30")
        self.output_dir_var = tk.StringVar(value=os.path.join(os.getcwd(), "downloads"))

        self._build_ui()
        self._poll_log_queue()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        dataset_group = ttk.LabelFrame(frame, text="1) 选择数据集（可多选）", padding=10)
        dataset_group.pack(fill=tk.X, pady=6)
        for idx, dataset in enumerate(DEFAULT_DATASETS):
            var = tk.BooleanVar(value=True)
            self.dataset_vars[dataset] = var
            ttk.Checkbutton(dataset_group, text=dataset, variable=var).grid(
                row=0, column=idx, padx=8, pady=4, sticky=tk.W
            )

        filter_group = ttk.LabelFrame(frame, text="2) 检索条件", padding=10)
        filter_group.pack(fill=tk.X, pady=6)

        ttk.Label(filter_group, text="开始日期 (YYYY-MM-DD)").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.start_date_var, width=18).grid(row=0, column=1, padx=6)

        ttk.Label(filter_group, text="结束日期 (YYYY-MM-DD)").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.end_date_var, width=18).grid(row=0, column=3, padx=6)

        ttk.Label(filter_group, text="BBox (minLon,minLat,maxLon,maxLat)").grid(row=1, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.bbox_var, width=45).grid(row=1, column=1, columnspan=3, padx=6, sticky=tk.W)

        ttk.Label(filter_group, text="每数据集最多下载景数").grid(row=2, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.max_items_var, width=18).grid(row=2, column=1, padx=6, sticky=tk.W)

        ttk.Label(filter_group, text="最大云量(%) 仅光学数据生效").grid(row=2, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.cloud_cover_var, width=18).grid(row=2, column=3, padx=6, sticky=tk.W)

        output_group = ttk.LabelFrame(frame, text="3) 输出目录", padding=10)
        output_group.pack(fill=tk.X, pady=6)

        ttk.Entry(output_group, textvariable=self.output_dir_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(output_group, text="浏览", command=self.choose_output_dir).pack(side=tk.LEFT, padx=6)

        action_group = ttk.Frame(frame)
        action_group.pack(fill=tk.X, pady=6)
        self.start_button = ttk.Button(action_group, text="开始批量下载", command=self.start_download)
        self.start_button.pack(side=tk.LEFT)

        self.progress = ttk.Progressbar(action_group, mode="indeterminate")
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)

        log_group = ttk.LabelFrame(frame, text="运行日志", padding=10)
        log_group.pack(fill=tk.BOTH, expand=True, pady=6)

        self.log_text = tk.Text(log_group, height=18)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def choose_output_dir(self) -> None:
        selected = filedialog.askdirectory(title="选择输出目录")
        if selected:
            self.output_dir_var.set(selected)

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(f"[{timestamp}] {message}")

    def _poll_log_queue(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        finally:
            self.root.after(200, self._poll_log_queue)

    def _validate_inputs(self) -> tuple[list[str], list[float], int, int]:
        datasets = [k for k, v in self.dataset_vars.items() if v.get()]
        if not datasets:
            raise ValueError("请至少选择一个数据集。")

        for value in (self.start_date_var.get(), self.end_date_var.get()):
            datetime.strptime(value, "%Y-%m-%d")

        bbox_parts = [x.strip() for x in self.bbox_var.get().split(",")]
        if len(bbox_parts) != 4:
            raise ValueError("BBox格式错误，应为: minLon,minLat,maxLon,maxLat")
        bbox = [float(v) for v in bbox_parts]
        if bbox[0] >= bbox[2] or bbox[1] >= bbox[3]:
            raise ValueError("BBox范围错误：min必须小于max。")

        max_items = int(self.max_items_var.get())
        cloud = int(self.cloud_cover_var.get())
        if max_items <= 0:
            raise ValueError("每数据集最多景数必须大于0。")
        if not 0 <= cloud <= 100:
            raise ValueError("云量范围应在0-100之间。")

        return datasets, bbox, max_items, cloud

    def start_download(self) -> None:
        if self.is_running:
            messagebox.showinfo("提示", "任务正在运行，请稍候。")
            return

        try:
            datasets, bbox, max_items, cloud = self._validate_inputs()
        except ValueError as exc:
            messagebox.showerror("输入错误", str(exc))
            return

        self.is_running = True
        self.start_button.configure(state=tk.DISABLED)
        self.progress.start(10)

        thread = threading.Thread(
            target=self._run_download,
            args=(datasets, bbox, max_items, cloud, self.output_dir_var.get()),
            daemon=True,
        )
        thread.start()

    def _run_download(self, datasets: list[str], bbox: list[float], max_items: int, cloud: int, output_dir: str) -> None:
        try:
            os.makedirs(output_dir, exist_ok=True)
            total = 0
            for dataset in datasets:
                self.log(f"开始检索数据集: {dataset}")
                items = self.search_items(dataset=dataset, bbox=bbox, max_items=max_items, cloud=cloud)
                self.log(f"{dataset} 检索到 {len(items)} 条记录")
                tasks = self.build_tasks(dataset, items, output_dir)
                self.log(f"{dataset} 计划下载 {len(tasks)} 个文件")
                for idx, task in enumerate(tasks, start=1):
                    self.download_file(task, idx, len(tasks))
                    total += 1

            self.log(f"全部完成，共下载 {total} 个文件。")
            messagebox.showinfo("完成", f"下载完成，共 {total} 个文件。")
        except Exception as exc:  # noqa: BLE001
            self.log(f"任务失败: {exc}")
            messagebox.showerror("任务失败", str(exc))
        finally:
            self.root.after(0, self._on_task_finished)

    def _on_task_finished(self) -> None:
        self.is_running = False
        self.start_button.configure(state=tk.NORMAL)
        self.progress.stop()

    def search_items(self, dataset: str, bbox: list[float], max_items: int, cloud: int) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "collections": [dataset],
            "bbox": bbox,
            "datetime": f"{self.start_date_var.get()}T00:00:00Z/{self.end_date_var.get()}T23:59:59Z",
            "limit": max_items,
            "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        }

        if dataset in {"sentinel-2-l2a", "landsat-c2-l2"}:
            payload["query"] = {"eo:cloud_cover": {"lte": cloud}}

        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            STAC_SEARCH_URL,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError) as exc:
            raise RuntimeError(f"检索失败({dataset}): {exc}") from exc

        features = data.get("features", [])
        if not isinstance(features, list):
            return []
        return features

    def build_tasks(self, dataset: str, items: list[dict[str, Any]], output_dir: str) -> list[DownloadTask]:
        tasks: list[DownloadTask] = []
        prefer_assets = DEFAULT_ASSET_PREFERENCE.get(dataset, [])
        dataset_dir = os.path.join(output_dir, dataset)
        os.makedirs(dataset_dir, exist_ok=True)

        for item in items:
            item_id = item.get("id", "unknown_item")
            assets = item.get("assets", {})
            selected_asset_key = None
            selected_asset_url = None

            for key in prefer_assets:
                meta = assets.get(key)
                if meta and isinstance(meta, dict) and meta.get("href"):
                    selected_asset_key = key
                    selected_asset_url = meta["href"]
                    break

            if not selected_asset_url:
                continue

            ext = os.path.splitext(selected_asset_url.split("?")[0])[1] or ".tif"
            safe_id = item_id.replace("/", "_")
            filename = f"{safe_id}_{selected_asset_key}{ext}"
            path = os.path.join(dataset_dir, filename)
            tasks.append(
                DownloadTask(
                    dataset=dataset,
                    item_id=item_id,
                    asset_key=selected_asset_key or "asset",
                    url=selected_asset_url,
                    output_path=path,
                )
            )
        return tasks

    def download_file(self, task: DownloadTask, index: int, total: int) -> None:
        if os.path.exists(task.output_path):
            self.log(f"[{index}/{total}] 已存在，跳过: {task.output_path}")
            return

        os.makedirs(os.path.dirname(task.output_path), exist_ok=True)
        self.log(f"[{index}/{total}] 下载中: {task.dataset} / {task.item_id} / {task.asset_key}")

        req = request.Request(task.url, headers={"User-Agent": "RS-Batch-Downloader/1.0"})
        try:
            with request.urlopen(req, timeout=180) as resp, open(task.output_path, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 512)
                    if not chunk:
                        break
                    f.write(chunk)
            self.log(f"[{index}/{total}] 下载完成: {task.output_path}")
        except (HTTPError, URLError, TimeoutError) as exc:
            self.log(f"[{index}/{total}] 下载失败: {task.url} ({exc})")


def main() -> None:
    root = tk.Tk()
    app = RemoteSensingDownloaderApp(root)
    root.minsize(920, 620)
    root.mainloop()


if __name__ == "__main__":
    main()
