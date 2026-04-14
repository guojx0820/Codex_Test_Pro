#!/usr/bin/env python3
"""遥感数据批量下载工具（GUI）。"""

from __future__ import annotations

import json
import os
import queue
import socket
import threading
import tkinter as tk
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

DEFAULT_STAC_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"
HISTORY_FILE = "task_history.json"

DATASET_CONFIG: dict[str, dict[str, Any]] = {
    "sentinel-2-l2a": {
        "collections": ["sentinel-2-l2a"],
        "asset_prefs": ["visual", "B04", "B03", "B02", "B08"],
        "cloud_filter": True,
    },
    "landsat-c2-l2": {
        "collections": ["landsat-c2-l2"],
        "asset_prefs": ["rendered_preview", "red", "green", "blue", "nir08", "qa_pixel"],
        "cloud_filter": True,
    },
    "sentinel-1-grd": {
        "collections": ["sentinel-1-grd"],
        "asset_prefs": ["vv", "vh", "thumbnail"],
        "cloud_filter": False,
    },
    "modis-13q1-061": {
        "collections": ["modis-13q1-061"],
        "asset_prefs": ["250m_16_days_NDVI", "250m_16_days_EVI", "thumbnail"],
        "cloud_filter": False,
    },
    "modis-09a1-061": {
        "collections": ["modis-09a1-061"],
        "asset_prefs": ["sur_refl_b01", "sur_refl_b02", "sur_refl_b03", "thumbnail"],
        "cloud_filter": False,
    },
    "goci-l2": {
        "collections": ["goci-l2"],
        "asset_prefs": ["chlor_a", "rrs_555", "thumbnail"],
        "cloud_filter": False,
    },
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
        self.root.title("遥感数据批量下载工具 v1.2")
        self.root.geometry("1120x760")

        self.log_queue: "queue.Queue[str]" = queue.Queue()
        self.is_running = False

        self.dataset_vars: dict[str, tk.BooleanVar] = {}
        self.start_date_var = tk.StringVar(value="2025-01-01")
        self.end_date_var = tk.StringVar(value="2025-01-31")
        self.aoi_mode_var = tk.StringVar(value="BBox")
        self.bbox_var = tk.StringVar(value="116.0,39.6,116.8,40.2")
        self.vector_path_var = tk.StringVar(value="")

        self.max_items_var = tk.StringVar(value="10")
        self.asset_limit_var = tk.StringVar(value="2")
        self.cloud_cover_var = tk.StringVar(value="30")
        self.retry_var = tk.StringVar(value="3")
        self.resume_var = tk.BooleanVar(value=True)

        self.output_dir_var = tk.StringVar(value=os.path.join(os.getcwd(), "downloads"))
        self.history_path = Path(os.getcwd()) / HISTORY_FILE

        self._build_ui()
        self._poll_log_queue()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)

        dataset_group = ttk.LabelFrame(frame, text="1) 选择数据集（可多选）", padding=10)
        dataset_group.pack(fill=tk.X, pady=6)

        columns = 3
        for idx, dataset in enumerate(DATASET_CONFIG):
            var = tk.BooleanVar(value=(idx < 3))
            self.dataset_vars[dataset] = var
            ttk.Checkbutton(dataset_group, text=dataset, variable=var).grid(
                row=idx // columns, column=idx % columns, padx=8, pady=4, sticky=tk.W
            )

        filter_group = ttk.LabelFrame(frame, text="2) 检索条件", padding=10)
        filter_group.pack(fill=tk.X, pady=6)

        ttk.Label(filter_group, text="开始日期 (YYYY-MM-DD)").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.start_date_var, width=16).grid(row=0, column=1, padx=6)

        ttk.Label(filter_group, text="结束日期 (YYYY-MM-DD)").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.end_date_var, width=16).grid(row=0, column=3, padx=6)

        ttk.Label(filter_group, text="空间范围输入方式").grid(row=1, column=0, sticky=tk.W)
        ttk.Combobox(
            filter_group,
            textvariable=self.aoi_mode_var,
            values=["BBox", "GeoJSON", "Shapefile"],
            state="readonly",
            width=14,
        ).grid(row=1, column=1, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="BBox (minLon,minLat,maxLon,maxLat)").grid(row=2, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.bbox_var, width=48).grid(row=2, column=1, columnspan=3, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="GeoJSON/Shapefile 路径").grid(row=3, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.vector_path_var, width=68).grid(row=3, column=1, columnspan=2, sticky=tk.W, padx=6)
        ttk.Button(filter_group, text="浏览", command=self.choose_vector_file).grid(row=3, column=3, sticky=tk.W)

        ttk.Label(filter_group, text="每数据集最多景数").grid(row=4, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.max_items_var, width=16).grid(row=4, column=1, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="每景下载资产数").grid(row=4, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.asset_limit_var, width=16).grid(row=4, column=3, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="最大云量(%) 仅光学数据").grid(row=5, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.cloud_cover_var, width=16).grid(row=5, column=1, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="失败重试次数").grid(row=5, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.retry_var, width=16).grid(row=5, column=3, sticky=tk.W, padx=6)
        ttk.Checkbutton(filter_group, text="启用断点续传", variable=self.resume_var).grid(row=6, column=0, sticky=tk.W)

        output_group = ttk.LabelFrame(frame, text="3) 输出目录", padding=10)
        output_group.pack(fill=tk.X, pady=6)
        ttk.Entry(output_group, textvariable=self.output_dir_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(output_group, text="浏览", command=self.choose_output_dir).pack(side=tk.LEFT, padx=6)

        action_group = ttk.Frame(frame)
        action_group.pack(fill=tk.X, pady=6)
        self.start_button = ttk.Button(action_group, text="开始批量下载", command=self.start_download)
        self.start_button.pack(side=tk.LEFT)
        ttk.Button(action_group, text="查看历史任务", command=self.show_history).pack(side=tk.LEFT, padx=8)

        self.progress = ttk.Progressbar(action_group, mode="indeterminate")
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)

        log_group = ttk.LabelFrame(frame, text="运行日志", padding=10)
        log_group.pack(fill=tk.BOTH, expand=True, pady=6)
        self.log_text = tk.Text(log_group, height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def choose_output_dir(self) -> None:
        selected = filedialog.askdirectory(title="选择输出目录")
        if selected:
            self.output_dir_var.set(selected)

    def choose_vector_file(self) -> None:
        selected = filedialog.askopenfilename(
            title="选择矢量文件",
            filetypes=[("Vector", "*.geojson *.json *.shp"), ("All", "*.*")],
        )
        if selected:
            self.vector_path_var.set(selected)

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

    def _load_geometry(self) -> tuple[list[float] | None, dict[str, Any] | None]:
        mode = self.aoi_mode_var.get()
        if mode == "BBox":
            bbox_parts = [x.strip() for x in self.bbox_var.get().split(",")]
            if len(bbox_parts) != 4:
                raise ValueError("BBox格式错误，应为: minLon,minLat,maxLon,maxLat")
            bbox = [float(v) for v in bbox_parts]
            if bbox[0] >= bbox[2] or bbox[1] >= bbox[3]:
                raise ValueError("BBox范围错误：min必须小于max。")
            return bbox, None

        path = self.vector_path_var.get().strip()
        if not path:
            raise ValueError("请先选择GeoJSON或Shapefile文件。")

        if mode == "GeoJSON":
            with open(path, "r", encoding="utf-8") as f:
                gj = json.load(f)
            if gj.get("type") == "FeatureCollection":
                if not gj.get("features"):
                    raise ValueError("GeoJSON中无要素。")
                geom = gj["features"][0].get("geometry")
            elif gj.get("type") == "Feature":
                geom = gj.get("geometry")
            else:
                geom = gj
            if not geom or "type" not in geom or "coordinates" not in geom:
                raise ValueError("GeoJSON几何无效。")
            return None, geom

        # Shapefile
        try:
            import shapefile  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise ValueError("读取Shapefile需要安装 pyshp：pip install pyshp") from exc

        reader = shapefile.Reader(path)
        shapes = reader.shapes()
        if not shapes:
            raise ValueError("Shapefile中无几何对象。")
        points = shapes[0].points
        if len(points) < 3:
            raise ValueError("Shapefile首个几何点数不足。")
        if points[0] != points[-1]:
            points.append(points[0])
        geom = {"type": "Polygon", "coordinates": [[list(p) for p in points]]}
        return None, geom

    def _validate_inputs(self) -> tuple[list[str], list[float] | None, dict[str, Any] | None, int, int, int, int]:
        datasets = [k for k, v in self.dataset_vars.items() if v.get()]
        if not datasets:
            raise ValueError("请至少选择一个数据集。")

        for value in (self.start_date_var.get(), self.end_date_var.get()):
            datetime.strptime(value, "%Y-%m-%d")

        bbox, geometry = self._load_geometry()

        max_items = int(self.max_items_var.get())
        asset_limit = int(self.asset_limit_var.get())
        cloud = int(self.cloud_cover_var.get())
        retry = int(self.retry_var.get())
        if max_items <= 0:
            raise ValueError("每数据集最多景数必须大于0。")
        if asset_limit <= 0:
            raise ValueError("每景下载资产数必须大于0。")
        if not 0 <= cloud <= 100:
            raise ValueError("云量范围应在0-100之间。")
        if retry < 0:
            raise ValueError("重试次数不能小于0。")

        return datasets, bbox, geometry, max_items, asset_limit, cloud, retry

    def start_download(self) -> None:
        if self.is_running:
            messagebox.showinfo("提示", "任务正在运行，请稍候。")
            return

        try:
            datasets, bbox, geometry, max_items, asset_limit, cloud, retry = self._validate_inputs()
        except ValueError as exc:
            messagebox.showerror("输入错误", str(exc))
            return

        self.is_running = True
        self.start_button.configure(state=tk.DISABLED)
        self.progress.start(10)

        thread = threading.Thread(
            target=self._run_download,
            args=(datasets, bbox, geometry, max_items, asset_limit, cloud, retry, self.output_dir_var.get()),
            daemon=True,
        )
        thread.start()

    def _run_download(
        self,
        datasets: list[str],
        bbox: list[float] | None,
        geometry: dict[str, Any] | None,
        max_items: int,
        asset_limit: int,
        cloud: int,
        retry: int,
        output_dir: str,
    ) -> None:
        ok_count = 0
        fail_count = 0
        history_records: list[dict[str, Any]] = []

        try:
            os.makedirs(output_dir, exist_ok=True)
            for dataset in datasets:
                self.log(f"开始检索数据集: {dataset}")
                items = self.search_items(dataset=dataset, bbox=bbox, geometry=geometry, max_items=max_items, cloud=cloud)
                self.log(f"{dataset} 检索到 {len(items)} 条记录")

                tasks = self.build_tasks(dataset, items, output_dir, asset_limit)
                self.log(f"{dataset} 计划下载 {len(tasks)} 个文件")
                if not tasks:
                    self.log(f"{dataset} 无可下载资产，请检查数据集是否可用或更换时间/区域。")

                for idx, task in enumerate(tasks, start=1):
                    success = self.download_file(task, idx, len(tasks), retry, self.resume_var.get())
                    history_records.append({**asdict(task), "success": success, "time": datetime.now().isoformat()})
                    if success:
                        ok_count += 1
                    else:
                        fail_count += 1

            self.save_history(history_records)
            summary = f"全部完成，成功 {ok_count}，失败 {fail_count}。"
            self.log(summary)
            messagebox.showinfo("完成", summary)
        except Exception as exc:  # noqa: BLE001
            self.log(f"任务失败: {exc}")
            messagebox.showerror("任务失败", str(exc))
        finally:
            self.root.after(0, self._on_task_finished)

    def _on_task_finished(self) -> None:
        self.is_running = False
        self.start_button.configure(state=tk.NORMAL)
        self.progress.stop()

    def _normalize_asset_url(self, url: str) -> str:
        """将 s3://bucket/key 转换为公共 HTTPS URL；其他 URL 原样返回。"""
        if url.startswith("s3://"):
            # s3://bucket/path/file.tif -> https://bucket.s3.amazonaws.com/path/file.tif
            parsed = parse.urlparse(url)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            return f"https://{bucket}.s3.amazonaws.com/{key}"
        return url

    def _pick_asset_url(self, asset_meta: dict[str, Any]) -> str | None:
        href = asset_meta.get("href")
        if isinstance(href, str) and href:
            return self._normalize_asset_url(href)

        # 兼容某些 STAC alternate 链接
        alternate = asset_meta.get("alternate", {})
        if isinstance(alternate, dict):
            for key in ("https", "s3"):
                info = alternate.get(key)
                if isinstance(info, dict) and isinstance(info.get("href"), str):
                    return self._normalize_asset_url(info["href"])
        return None

    def search_items(
        self,
        dataset: str,
        bbox: list[float] | None,
        geometry: dict[str, Any] | None,
        max_items: int,
        cloud: int,
    ) -> list[dict[str, Any]]:
        conf = DATASET_CONFIG.get(dataset)
        if not conf:
            raise RuntimeError(f"未配置数据集: {dataset}")

        payload: dict[str, Any] = {
            "collections": conf["collections"],
            "datetime": f"{self.start_date_var.get()}T00:00:00Z/{self.end_date_var.get()}T23:59:59Z",
            "limit": max_items,
            "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        }
        if bbox:
            payload["bbox"] = bbox
        if geometry:
            payload["intersects"] = geometry

        if conf.get("cloud_filter"):
            payload["query"] = {"eo:cloud_cover": {"lte": cloud}}

        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            DEFAULT_STAC_SEARCH_URL,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "RS-Batch-Downloader/1.2"},
        )
        try:
            with request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError) as exc:
            raise RuntimeError(f"检索失败({dataset}): {exc}") from exc

        features = data.get("features", [])
        return features if isinstance(features, list) else []

    def build_tasks(self, dataset: str, items: list[dict[str, Any]], output_dir: str, asset_limit: int) -> list[DownloadTask]:
        tasks: list[DownloadTask] = []
        pref_assets = DATASET_CONFIG[dataset]["asset_prefs"]
        dataset_dir = os.path.join(output_dir, dataset)
        os.makedirs(dataset_dir, exist_ok=True)

        for item in items:
            item_id = item.get("id", "unknown_item")
            assets = item.get("assets", {})
            added = 0
            for key in pref_assets:
                if added >= asset_limit:
                    break
                meta = assets.get(key)
                if not isinstance(meta, dict):
                    continue
                url = self._pick_asset_url(meta)
                if not url:
                    continue

                ext = os.path.splitext(url.split("?")[0])[1] or ".tif"
                safe_id = str(item_id).replace("/", "_")
                filename = f"{safe_id}_{key}{ext}"
                path = os.path.join(dataset_dir, filename)
                tasks.append(DownloadTask(dataset=dataset, item_id=str(item_id), asset_key=key, url=url, output_path=path))
                added += 1
        return tasks

    def download_file(self, task: DownloadTask, index: int, total: int, retry: int, resume: bool) -> bool:
        attempts = retry + 1

        for attempt in range(1, attempts + 1):
            try:
                part_size = os.path.getsize(task.output_path) if (resume and os.path.exists(task.output_path)) else 0
                headers = {"User-Agent": "RS-Batch-Downloader/1.2"}
                mode = "wb"
                if part_size > 0:
                    headers["Range"] = f"bytes={part_size}-"
                    mode = "ab"
                    self.log(f"[{index}/{total}] 续传中: {task.asset_key} ({part_size} bytes)")
                else:
                    self.log(f"[{index}/{total}] 下载中: {task.dataset} / {task.item_id} / {task.asset_key}")

                req = request.Request(task.url, headers=headers)
                with request.urlopen(req, timeout=240) as resp:
                    status = getattr(resp, "status", 200)
                    if status == 200 and mode == "ab":
                        # 服务端不支持Range，重下
                        mode = "wb"
                    os.makedirs(os.path.dirname(task.output_path), exist_ok=True)
                    with open(task.output_path, mode) as f:
                        while True:
                            chunk = resp.read(1024 * 512)
                            if not chunk:
                                break
                            f.write(chunk)

                self.log(f"[{index}/{total}] 下载完成: {task.output_path}")
                return True
            except (HTTPError, URLError, TimeoutError, socket.timeout) as exc:
                if attempt < attempts:
                    self.log(f"[{index}/{total}] 第 {attempt} 次失败，准备重试: {exc}")
                else:
                    self.log(f"[{index}/{total}] 下载失败: {task.url} ({exc})")
            except Exception as exc:  # noqa: BLE001
                self.log(f"[{index}/{total}] 下载异常: {task.url} ({exc})")
                break
        return False

    def save_history(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        existing: list[dict[str, Any]] = []
        if self.history_path.exists():
            try:
                with open(self.history_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if not isinstance(existing, list):
                    existing = []
            except Exception:
                existing = []

        existing.extend(records)
        with open(self.history_path, "w", encoding="utf-8") as f:
            json.dump(existing[-2000:], f, ensure_ascii=False, indent=2)

    def show_history(self) -> None:
        if not self.history_path.exists():
            messagebox.showinfo("历史任务", "暂无历史任务。")
            return

        try:
            with open(self.history_path, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                raise ValueError("history format error")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("历史任务", f"读取历史失败: {exc}")
            return

        lines = []
        for r in records[-30:]:
            status = "成功" if r.get("success") else "失败"
            lines.append(f"{r.get('time', '')} | {status} | {r.get('dataset', '')} | {r.get('item_id', '')} | {r.get('asset_key', '')}")
        messagebox.showinfo("最近30条任务", "\n".join(lines) if lines else "暂无记录")


def main() -> None:
    root = tk.Tk()
    app = RemoteSensingDownloaderApp(root)
    root.minsize(1000, 700)
    root.mainloop()


if __name__ == "__main__":
    main()
