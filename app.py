#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import socket
import threading
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

DEFAULT_STAC_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"
CMR_GRANULES_API = "https://cmr.earthdata.nasa.gov/search/granules.json"
CMR_COLLECTIONS_API = "https://cmr.earthdata.nasa.gov/search/collections.json"
HISTORY_FILE = "task_history.json"

DATASET_CONFIG: dict[str, dict[str, Any]] = {
    "sentinel-2-l2a": {
        "provider": "stac",
        "collections": ["sentinel-2-l2a"],
        "asset_prefs": ["visual", "B04", "B03", "B02", "B08"],
        "cloud_filter": True,
    },
    "landsat-c2-l2": {
        "provider": "stac",
        "collections": ["landsat-c2-l2"],
        "asset_prefs": ["rendered_preview", "red", "green", "blue", "nir08", "qa_pixel"],
        "cloud_filter": True,
    },
    "sentinel-1-grd": {
        "provider": "stac",
        "collections": ["sentinel-1-grd"],
        "asset_prefs": ["vv", "vh", "thumbnail"],
        "cloud_filter": False,
    },
    "modis-13q1-061": {
        "provider": "cmr",
        "short_name": "MOD13Q1",
        "version": "061",
        "asset_prefs": ["NDVI", "EVI", "hdf"],
    },
    "modis-09a1-061": {
        "provider": "cmr",
        "short_name": "MOD09A1",
        "version": "061",
        "asset_prefs": ["sur_refl", "hdf"],
    },
    "goci-l2": {
        "provider": "cmr_keyword",
        "keyword": "GOCI L2",
        "asset_prefs": ["nc", "h5"],
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
        self.root.title("遥感数据批量下载工具 v1.3")
        self.root.geometry("1150x780")

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
        self.retry_var = tk.StringVar(value="2")
        self.workers_var = tk.StringVar(value="4")
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
        cols = 3
        for idx, dataset in enumerate(DATASET_CONFIG):
            var = tk.BooleanVar(value=(idx < 3))
            self.dataset_vars[dataset] = var
            ttk.Checkbutton(dataset_group, text=dataset, variable=var).grid(row=idx // cols, column=idx % cols, sticky=tk.W, padx=8, pady=4)

        filter_group = ttk.LabelFrame(frame, text="2) 检索条件", padding=10)
        filter_group.pack(fill=tk.X, pady=6)

        ttk.Label(filter_group, text="开始日期").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.start_date_var, width=16).grid(row=0, column=1, padx=6)
        ttk.Label(filter_group, text="结束日期").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.end_date_var, width=16).grid(row=0, column=3, padx=6)

        ttk.Label(filter_group, text="空间输入").grid(row=1, column=0, sticky=tk.W)
        ttk.Combobox(filter_group, textvariable=self.aoi_mode_var, values=["BBox", "GeoJSON", "Shapefile"], state="readonly", width=12).grid(row=1, column=1, sticky=tk.W, padx=6)
        ttk.Label(filter_group, text="BBox").grid(row=2, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.bbox_var, width=52).grid(row=2, column=1, columnspan=3, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="GeoJSON/Shapefile").grid(row=3, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.vector_path_var, width=70).grid(row=3, column=1, columnspan=2, sticky=tk.W, padx=6)
        ttk.Button(filter_group, text="浏览", command=self.choose_vector_file).grid(row=3, column=3, sticky=tk.W)

        ttk.Label(filter_group, text="每数据集景数").grid(row=4, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.max_items_var, width=16).grid(row=4, column=1, sticky=tk.W, padx=6)
        ttk.Label(filter_group, text="每景资产数").grid(row=4, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.asset_limit_var, width=16).grid(row=4, column=3, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="最大云量%").grid(row=5, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.cloud_cover_var, width=16).grid(row=5, column=1, sticky=tk.W, padx=6)
        ttk.Label(filter_group, text="失败重试次数").grid(row=5, column=2, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.retry_var, width=16).grid(row=5, column=3, sticky=tk.W, padx=6)

        ttk.Label(filter_group, text="并发下载线程").grid(row=6, column=0, sticky=tk.W)
        ttk.Entry(filter_group, textvariable=self.workers_var, width=16).grid(row=6, column=1, sticky=tk.W, padx=6)
        ttk.Checkbutton(filter_group, text="启用断点续传", variable=self.resume_var).grid(row=6, column=2, sticky=tk.W)

        out_group = ttk.LabelFrame(frame, text="3) 输出目录", padding=10)
        out_group.pack(fill=tk.X, pady=6)
        ttk.Entry(out_group, textvariable=self.output_dir_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(out_group, text="浏览", command=self.choose_output_dir).pack(side=tk.LEFT, padx=6)

        action = ttk.Frame(frame)
        action.pack(fill=tk.X, pady=6)
        self.start_button = ttk.Button(action, text="开始批量下载", command=self.start_download)
        self.start_button.pack(side=tk.LEFT)
        ttk.Button(action, text="查看历史任务", command=self.show_history).pack(side=tk.LEFT, padx=8)
        self.progress = ttk.Progressbar(action, mode="indeterminate")
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
        selected = filedialog.askopenfilename(title="选择矢量文件", filetypes=[("Vector", "*.geojson *.json *.shp"), ("All", "*.*")])
        if selected:
            self.vector_path_var.set(selected)

    def log(self, message: str) -> None:
        self.log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def _poll_log_queue(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        finally:
            self.root.after(150, self._poll_log_queue)

    def _load_geometry(self) -> tuple[list[float] | None, dict[str, Any] | None]:
        mode = self.aoi_mode_var.get()
        if mode == "BBox":
            parts = [x.strip() for x in self.bbox_var.get().split(",")]
            if len(parts) != 4:
                raise ValueError("BBox格式错误")
            bbox = [float(v) for v in parts]
            return bbox, None

        path = self.vector_path_var.get().strip()
        if not path:
            raise ValueError("请选择矢量文件")

        if mode == "GeoJSON":
            with open(path, "r", encoding="utf-8") as f:
                gj = json.load(f)
            if gj.get("type") == "FeatureCollection":
                geom = gj["features"][0]["geometry"]
            elif gj.get("type") == "Feature":
                geom = gj["geometry"]
            else:
                geom = gj
            return None, geom

        try:
            import shapefile  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Shapefile需要pyshp: pip install pyshp") from exc

        reader = shapefile.Reader(path)
        shp = reader.shapes()[0]
        points = shp.points
        if points[0] != points[-1]:
            points.append(points[0])
        return None, {"type": "Polygon", "coordinates": [[list(p) for p in points]]}

    def _validate_inputs(self) -> tuple[list[str], list[float] | None, dict[str, Any] | None, int, int, int, int, int]:
        datasets = [k for k, v in self.dataset_vars.items() if v.get()]
        if not datasets:
            raise ValueError("至少选择一个数据集")
        datetime.strptime(self.start_date_var.get(), "%Y-%m-%d")
        datetime.strptime(self.end_date_var.get(), "%Y-%m-%d")
        bbox, geometry = self._load_geometry()

        max_items = max(1, int(self.max_items_var.get()))
        asset_limit = max(1, int(self.asset_limit_var.get()))
        cloud = int(self.cloud_cover_var.get())
        retry = max(0, int(self.retry_var.get()))
        workers = min(16, max(1, int(self.workers_var.get())))
        return datasets, bbox, geometry, max_items, asset_limit, cloud, retry, workers

    def start_download(self) -> None:
        if self.is_running:
            messagebox.showinfo("提示", "任务进行中")
            return

        try:
            args = self._validate_inputs()
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
        hist: list[dict[str, Any]] = []

        try:
            os.makedirs(output_dir, exist_ok=True)
            all_tasks: list[DownloadTask] = []
            for dataset in datasets:
                self.log(f"开始检索数据集: {dataset}")
                items = self.search_items(dataset, bbox, geometry, max_items, cloud)
                self.log(f"{dataset} 检索到 {len(items)} 条记录")
                tasks = self.build_tasks(dataset, items, output_dir, asset_limit)
                self.log(f"{dataset} 计划下载 {len(tasks)} 个文件")
                if not tasks:
                    self.log(f"{dataset} 无可下载资产")
                all_tasks.extend(tasks)

            total = len(all_tasks)
            if total == 0:
                self.log("没有可下载文件，请调整条件。")
            else:
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futures = {
                        ex.submit(self.download_file, task, idx + 1, total, retry, self.resume_var.get()): task
                        for idx, task in enumerate(all_tasks)
                    }
                    for fut in as_completed(futures):
                        task = futures[fut]
                        success = False
                        try:
                            success = fut.result()
                        except Exception as exc:  # noqa: BLE001
                            self.log(f"下载线程异常: {task.url} ({exc})")
                        hist.append({**asdict(task), "success": success, "time": datetime.now().isoformat()})
                        if success:
                            ok += 1
                        else:
                            fail += 1

            self.save_history(hist)
            msg = f"全部完成，成功 {ok}，失败 {fail}。"
            self.log(msg)
            messagebox.showinfo("完成", msg)
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
        if url.startswith("s3://"):
            p = parse.urlparse(url)
            return f"https://{p.netloc}.s3.amazonaws.com/{p.path.lstrip('/')}"
        return url

    def _pick_asset_url(self, asset_meta: dict[str, Any]) -> str | None:
        href = asset_meta.get("href")
        if isinstance(href, str) and href:
            return self._normalize_asset_url(href)
        alt = asset_meta.get("alternate", {})
        if isinstance(alt, dict):
            for key in ("https", "s3"):
                data = alt.get(key)
                if isinstance(data, dict) and isinstance(data.get("href"), str):
                    return self._normalize_asset_url(data["href"])
        return None

    def _cmr_query(self, params: dict[str, str]) -> dict[str, Any]:
        query = parse.urlencode(params)
        url = f"{CMR_GRANULES_API}?{query}"
        req = request.Request(url, headers={"User-Agent": "RS-Batch-Downloader/1.3"})
        with request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def search_items(
        self,
        dataset: str,
        bbox: list[float] | None,
        geometry: dict[str, Any] | None,
        max_items: int,
        cloud: int,
    ) -> list[dict[str, Any]]:
        conf = DATASET_CONFIG[dataset]
        provider = conf.get("provider", "stac")
        if geometry:
            # CMR granules API 这里简化为bbox，仅STAC支持intersects
            self.log("提示: 当前仅STAC数据集支持GeoJSON/Shapefile精确相交检索；CMR数据集将使用BBox。")

        if provider == "stac":
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

            req = request.Request(
                DEFAULT_STAC_SEARCH_URL,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json", "User-Agent": "RS-Batch-Downloader/1.3"},
            )
            with request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            feats = data.get("features", [])
            return feats if isinstance(feats, list) else []

        temporal = f"{self.start_date_var.get()}T00:00:00Z,{self.end_date_var.get()}T23:59:59Z"
        bb = ",".join(str(x) for x in (bbox or [-180, -90, 180, 90]))

        if provider == "cmr":
            params = {
                "short_name": conf["short_name"],
                "version": conf["version"],
                "page_size": str(max_items),
                "temporal": temporal,
                "bounding_box": bb,
            }
            data = self._cmr_query(params)
            entries = data.get("feed", {}).get("entry", [])
            return self._cmr_entries_to_items(entries)

        # cmr_keyword for goci
        cparams = {"keyword": conf.get("keyword", "GOCI"), "page_size": "5"}
        curl = f"{CMR_COLLECTIONS_API}?{parse.urlencode(cparams)}"
        req = request.Request(curl, headers={"User-Agent": "RS-Batch-Downloader/1.3"})
        with request.urlopen(req, timeout=90) as resp:
            cdata = json.loads(resp.read().decode("utf-8"))
        entries = cdata.get("feed", {}).get("entry", [])
        if not entries:
            return []
        short_name = entries[0].get("short_name")
        version = entries[0].get("version_id")
        if not short_name:
            return []
        params = {
            "short_name": short_name,
            "page_size": str(max_items),
            "temporal": temporal,
            "bounding_box": bb,
        }
        if version:
            params["version"] = version
        data = self._cmr_query(params)
        return self._cmr_entries_to_items(data.get("feed", {}).get("entry", []))

    def _cmr_entries_to_items(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for e in entries:
            gid = e.get("producer_granule_id") or e.get("id") or "cmr_item"
            assets: dict[str, dict[str, str]] = {}
            for link in e.get("links", []):
                href = link.get("href")
                if not isinstance(href, str):
                    continue
                # 过滤非数据链接
                rel = str(link.get("rel", ""))
                if "metadata" in rel:
                    continue
                title = str(link.get("title") or os.path.basename(parse.urlparse(href).path) or "asset")
                key = re.sub(r"[^A-Za-z0-9._-]+", "_", title)[:50] or "asset"
                if key in assets:
                    key = f"{key}_{len(assets)}"
                assets[key] = {"href": href}
            if assets:
                items.append({"id": gid, "assets": assets})
        return items

    def _safe_filename(self, item_id: str, asset_key: str, url: str) -> str:
        ext = os.path.splitext(parse.urlparse(url).path)[1] or ".dat"
        base = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{item_id}_{asset_key}")
        digest = hashlib.md5(f"{item_id}|{asset_key}|{url}".encode("utf-8")).hexdigest()[:8]
        base = (base[:60] if len(base) > 60 else base)
        return f"{base}_{digest}{ext}"

    def build_tasks(self, dataset: str, items: list[dict[str, Any]], output_dir: str, asset_limit: int) -> list[DownloadTask]:
        tasks: list[DownloadTask] = []
        prefs = DATASET_CONFIG[dataset]["asset_prefs"]
        ds_dir = os.path.join(output_dir, dataset)
        os.makedirs(ds_dir, exist_ok=True)

        for item in items:
            item_id = str(item.get("id", "unknown_item"))
            assets = item.get("assets", {})
            selected = 0

            # 先按偏好匹配
            keys = list(assets.keys())
            ordered_keys: list[str] = []
            for p in prefs:
                matched = [k for k in keys if p.lower() in k.lower()]
                for m in matched:
                    if m not in ordered_keys:
                        ordered_keys.append(m)
            for k in keys:
                if k not in ordered_keys:
                    ordered_keys.append(k)

            for key in ordered_keys:
                if selected >= asset_limit:
                    break
                meta = assets.get(key)
                if not isinstance(meta, dict):
                    continue
                url = self._pick_asset_url(meta)
                if not url or not url.startswith(("http://", "https://")):
                    continue
                fname = self._safe_filename(item_id, key, url)
                path = os.path.join(ds_dir, fname)
                tasks.append(DownloadTask(dataset=dataset, item_id=item_id, asset_key=key, url=url, output_path=path))
                selected += 1
        return tasks

    def download_file(self, task: DownloadTask, index: int, total: int, retry: int, resume: bool) -> bool:
        attempts = retry + 1
        for attempt in range(1, attempts + 1):
            try:
                part = os.path.getsize(task.output_path) if (resume and os.path.exists(task.output_path)) else 0
                headers = {"User-Agent": "RS-Batch-Downloader/1.3"}
                mode = "wb"
                if part > 0:
                    headers["Range"] = f"bytes={part}-"
                    mode = "ab"
                    self.log(f"[{index}/{total}] 续传: {task.asset_key}")
                else:
                    self.log(f"[{index}/{total}] 下载中: {task.dataset}/{task.item_id}/{task.asset_key}")

                req = request.Request(task.url, headers=headers)
                with request.urlopen(req, timeout=120) as resp:
                    status = getattr(resp, "status", 200)
                    if status == 200 and mode == "ab":
                        mode = "wb"
                    os.makedirs(os.path.dirname(task.output_path), exist_ok=True)
                    with open(task.output_path, mode) as f:
                        while True:
                            chunk = resp.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                self.log(f"[{index}/{total}] 完成: {os.path.basename(task.output_path)}")
                return True
            except FileNotFoundError:
                # Windows 长路径/非法路径兜底：再缩短一次
                short_name = hashlib.md5(task.url.encode("utf-8")).hexdigest() + ".bin"
                task.output_path = os.path.join(os.path.dirname(task.output_path), short_name)
            except (HTTPError, URLError, TimeoutError, socket.timeout) as exc:
                if attempt < attempts:
                    self.log(f"[{index}/{total}] 失败重试({attempt}/{attempts-1}): {exc}")
                else:
                    self.log(f"[{index}/{total}] 下载失败: {exc}")
            except Exception as exc:  # noqa: BLE001
                self.log(f"[{index}/{total}] 下载异常: {exc}")
                break
        return False

    def save_history(self, records: list[dict[str, Any]]) -> None:
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
            messagebox.showinfo("历史任务", "暂无历史任务")
            return
        try:
            records = json.loads(self.history_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("历史任务", f"读取失败: {exc}")
            return
        lines = []
        for r in records[-30:]:
            lines.append(f"{r.get('time','')} | {'成功' if r.get('success') else '失败'} | {r.get('dataset','')} | {r.get('asset_key','')}")
        messagebox.showinfo("最近30条", "\n".join(lines) if lines else "暂无")


def main() -> None:
    root = tk.Tk()
    RemoteSensingDownloaderApp(root)
    root.minsize(1024, 700)
    root.mainloop()


if __name__ == "__main__":
    main()
