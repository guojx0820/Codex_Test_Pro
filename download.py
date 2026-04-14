#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlparse

import requests

STAC_SEARCH = "https://earth-search.aws.element84.com/v1/search"
CMR_GRANULES = "https://cmr.earthdata.nasa.gov/search/granules.json"
EARTHDATA_PROFILE = "https://urs.earthdata.nasa.gov/profile"

FILTER_KEYWORDS = ["schema", "metadata", "thumbnail", "overview"]
VALID_EXT = (".hdf", ".tif", ".tiff", ".zip", ".safe")


@dataclass
class DownloadTask:
    dataset: str
    item_id: str
    asset_key: str
    url: str
    output_path: str
    file_type: str
    requires_auth: bool = False


class DownloadEngine:
    def __init__(self, logger: Callable[[str], None]) -> None:
        self.log = logger

    def build_session(self, nasa_user: str, nasa_password: str, nasa_token: str) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": "RS-Batch-Downloader/1.5"})
        if nasa_token.strip():
            session.headers.update({"Authorization": f"Bearer {nasa_token.strip()}"})
            self.log("NASA认证: 使用Token")
        elif nasa_user.strip() and nasa_password:
            session.auth = (nasa_user.strip(), nasa_password)
            self.log("NASA认证: 使用用户名/密码")
        else:
            self.log("NASA认证: 未配置（MODIS可能401/403）")
        return session

    def verify_auth(self, session: requests.Session) -> bool:
        try:
            r = session.get(EARTHDATA_PROFILE, timeout=20, allow_redirects=True)
            ok = r.status_code < 400
            self.log(f"NASA认证检测: {'成功' if ok else '失败'} (HTTP {r.status_code})")
            return ok
        except requests.RequestException as exc:
            self.log(f"NASA认证检测异常: {exc}")
            return False

    def search_dataset(
        self,
        session: requests.Session,
        dataset: str,
        bbox: list[float] | None,
        geometry: dict[str, Any] | None,
        start_date: str,
        end_date: str,
        max_items: int,
        cloud: int,
    ) -> list[dict[str, Any]]:
        if dataset.startswith("modis"):
            return self._search_modis_cmr(session, dataset, bbox, start_date, end_date, max_items)
        return self._search_stac(dataset, bbox, geometry, start_date, end_date, max_items, cloud)

    def _search_stac(
        self,
        dataset: str,
        bbox: list[float] | None,
        geometry: dict[str, Any] | None,
        start_date: str,
        end_date: str,
        max_items: int,
        cloud: int,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "collections": [dataset],
            "datetime": f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
            "limit": max_items,
            "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        }
        if bbox:
            payload["bbox"] = bbox
        if geometry:
            payload["intersects"] = geometry
        if dataset in ["sentinel-2-l2a", "landsat-c2-l2"]:
            payload["query"] = {"eo:cloud_cover": {"lte": cloud}}

        r = requests.post(STAC_SEARCH, json=payload, timeout=45)
        r.raise_for_status()
        return r.json().get("features", [])

    def _search_modis_cmr(
        self,
        session: requests.Session,
        dataset: str,
        bbox: list[float] | None,
        start_date: str,
        end_date: str,
        max_items: int,
    ) -> list[dict[str, Any]]:
        short_name = "MOD13Q1" if dataset == "modis-13q1-061" else "MOD09A1"
        params = {
            "short_name": short_name,
            "version": "061",
            "page_size": str(max_items),
            "temporal": f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
            "bounding_box": ",".join(str(x) for x in (bbox or [-180, -90, 180, 90])),
        }
        r = session.get(CMR_GRANULES, params=params, timeout=45)
        r.raise_for_status()
        entries = r.json().get("feed", {}).get("entry", [])

        items: list[dict[str, Any]] = []
        for e in entries:
            item_id = e.get("producer_granule_id") or e.get("id") or "modis"
            assets: dict[str, dict[str, str]] = {}
            for i, link in enumerate(e.get("links", [])):
                href = str(link.get("href", ""))
                if self._is_valid_href(href):
                    assets[f"cmr_{i}"] = {"href": href}
            if assets:
                items.append({"id": item_id, "assets": assets})
        return items

    def build_tasks(self, dataset: str, items: list[dict[str, Any]], out_dir: str, asset_limit: int) -> tuple[list[DownloadTask], int]:
        ds_dir = os.path.join(out_dir, dataset)
        os.makedirs(ds_dir, exist_ok=True)

        tasks: list[DownloadTask] = []
        filtered = 0

        for item in items:
            item_id = str(item.get("id", "item"))
            assets = item.get("assets", {})
            selected = 0
            for asset_key, asset in assets.items():
                href = self._asset_href(asset)
                if not href:
                    self.log(f"过滤资产: {asset_key} | href=空 | 原因=无有效链接")
                    filtered += 1
                    continue

                key_l = str(asset_key).lower()
                if any(x in key_l for x in FILTER_KEYWORDS):
                    self.log(f"过滤资产: {asset_key} | href={href} | 原因=黑名单key")
                    filtered += 1
                    continue

                href_l = href.lower()
                if not self._is_valid_href(href):
                    self.log(f"过滤资产: {asset_key} | href={href} | 原因=无效链接")
                    filtered += 1
                    continue

                # Sentinel-1 仅下载影像数据
                if dataset == "sentinel-1-grd" and not href_l.endswith((".tiff", ".tif", ".zip", ".safe")):
                    self.log(f"过滤资产: {asset_key} | href={href} | 原因=非影像")
                    filtered += 1
                    continue

                ext = self._ext_from_href(href_l)
                if ext == "":
                    self.log(f"过滤资产: {asset_key} | href={href} | 原因=未知扩展名")
                    filtered += 1
                    continue

                filename = self._safe_filename(item_id, str(asset_key), href, ext)
                tasks.append(
                    DownloadTask(
                        dataset=dataset,
                        item_id=item_id,
                        asset_key=str(asset_key),
                        url=href,  # 严禁拼接/修改 href
                        output_path=os.path.join(ds_dir, filename),
                        file_type=ext,
                        requires_auth=dataset.startswith("modis"),
                    )
                )
                selected += 1
                if selected >= asset_limit:
                    break

        return tasks, filtered

    def _asset_href(self, asset: Any) -> str | None:
        if not isinstance(asset, dict):
            return None

        # Sentinel-1/Landsat 优先使用 STAC 中原生 https alternate，避免手工拼接S3路径
        alternate = asset.get("alternate", {})
        if isinstance(alternate, dict):
            https_node = alternate.get("https")
            if isinstance(https_node, dict):
                href = https_node.get("href")
                if isinstance(href, str) and href.startswith("http"):
                    return href

        href = asset.get("href")
        if isinstance(href, str) and href.startswith("http"):
            return href

        return None

    def _ext_from_href(self, href_l: str) -> str:
        path = urlparse(href_l).path.lower()
        for ext in VALID_EXT:
            if path.endswith(ext):
                return ext
        return ""

    def _is_valid_href(self, href: str) -> bool:
        h = href.lower()
        if "this_link" in h:
            return False
        if h.endswith(".html") or "text/html" in h:
            return False
        return self._ext_from_href(h) != ""

    def _safe_filename(self, item_id: str, asset_key: str, href: str, ext: str) -> str:
        base = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{item_id}_{asset_key}")[:50]
        digest = hashlib.md5(f"{item_id}|{asset_key}|{href}".encode("utf-8")).hexdigest()[:8]
        return f"{base}_{digest}{ext}"

    def download_one(self, session: requests.Session, task: DownloadTask, retry: int, resume: bool) -> bool:
        attempts = retry + 1
        for attempt in range(attempts):
            try:
                if task.dataset.startswith("modis"):
                    return self._download_with_session(session, task.url, task.output_path)
                return self._download_with_requests(task.url, task.output_path, resume)
            except requests.RequestException as exc:
                if attempt == attempts - 1:
                    self.log(f"下载失败: {task.url} ({exc})")
            except OSError as exc:
                self.log(f"文件写入失败: {task.output_path} ({exc})")
                return False
        return False

    def _download_with_session(self, session: requests.Session, url: str, path: str) -> bool:
        r = session.get(url, stream=True, timeout=120, allow_redirects=True)
        r.raise_for_status()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return True

    def _download_with_requests(self, url: str, path: str, resume: bool) -> bool:
        headers: dict[str, str] = {"User-Agent": "RS-Batch-Downloader/1.5"}
        mode = "wb"
        existing = os.path.getsize(path) if (resume and os.path.exists(path)) else 0
        if existing > 0:
            headers["Range"] = f"bytes={existing}-"
            mode = "ab"

        r = requests.get(url, stream=True, timeout=120, headers=headers)
        r.raise_for_status()
        if r.status_code == 200 and mode == "ab":
            mode = "wb"

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode) as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        return True
