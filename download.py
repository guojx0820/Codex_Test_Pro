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
VALID_EXT = (".hdf", ".tif", ".tiff", ".zip")


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
        session.headers.update({"User-Agent": "RS-Batch-Downloader/1.4"})
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
            resp = session.get(EARTHDATA_PROFILE, timeout=20, allow_redirects=True)
            ok = resp.status_code < 400
            self.log(f"NASA认证检测: {'成功' if ok else '失败'} (HTTP {resp.status_code})")
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
        if dataset in ["modis-13q1-061", "modis-09a1-061"]:
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

        resp = requests.post(STAC_SEARCH, json=payload, timeout=45)
        resp.raise_for_status()
        return resp.json().get("features", [])

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
        resp = session.get(CMR_GRANULES, params=params, timeout=45)
        resp.raise_for_status()
        entries = resp.json().get("feed", {}).get("entry", [])

        items: list[dict[str, Any]] = []
        for e in entries:
            item_id = e.get("producer_granule_id") or e.get("id") or "modis"
            assets: dict[str, dict[str, str]] = {}
            for idx, link in enumerate(e.get("links", [])):
                href = str(link.get("href", ""))
                if self._is_valid_href(href):
                    key = f"cmr_{idx}"
                    assets[key] = {"href": href}
            if assets:
                items.append({"id": item_id, "assets": assets})
        return items

    def build_tasks(self, dataset: str, items: list[dict[str, Any]], out_dir: str, asset_limit: int) -> tuple[list[DownloadTask], int]:
        dataset_dir = os.path.join(out_dir, dataset)
        os.makedirs(dataset_dir, exist_ok=True)

        tasks: list[DownloadTask] = []
        filtered = 0
        for item in items:
            item_id = str(item.get("id", "item"))
            assets = item.get("assets", {})

            selected_assets = self._select_assets(dataset, assets)
            if asset_limit > 0:
                selected_assets = selected_assets[:asset_limit]

            for key, href in selected_assets:
                if not self._is_valid_href(href):
                    filtered += 1
                    continue
                ext = os.path.splitext(urlparse(href).path)[1].lower()
                if not ext and ".safe" in href.lower():
                    ext = ".SAFE"
                if ext not in VALID_EXT and ext != ".safe":
                    filtered += 1
                    continue

                filename = self._safe_filename(item_id, key, href, ext)
                tasks.append(
                    DownloadTask(
                        dataset=dataset,
                        item_id=item_id,
                        asset_key=key,
                        url=self._normalize_s3(href),
                        output_path=os.path.join(dataset_dir, filename),
                        file_type=ext or "unknown",
                        requires_auth=(dataset.startswith("modis-")),
                    )
                )

        return tasks, filtered

    def _select_assets(self, dataset: str, assets: dict[str, Any]) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []

        # Sentinel-1: 优先 data/product，且只留影像类
        if dataset == "sentinel-1-grd":
            for preferred in ["data", "product"]:
                m = assets.get(preferred)
                href = self._asset_href(m)
                if href and self._is_imagery_asset(preferred, href):
                    pairs.append((preferred, href))
            if pairs:
                return pairs

        for key, meta in assets.items():
            key_l = str(key).lower()
            if any(k in key_l for k in FILTER_KEYWORDS):
                continue
            href = self._asset_href(meta)
            if not href:
                continue
            if dataset == "sentinel-1-grd" and not self._is_imagery_asset(key, href):
                continue
            pairs.append((str(key), href))

        return pairs

    def _asset_href(self, meta: Any) -> str | None:
        if isinstance(meta, dict):
            href = meta.get("href")
            if isinstance(href, str) and href:
                return href
            alt = meta.get("alternate", {})
            if isinstance(alt, dict):
                for k in ["https", "s3"]:
                    node = alt.get(k)
                    if isinstance(node, dict) and isinstance(node.get("href"), str):
                        return node["href"]
        return None

    def _is_imagery_asset(self, key: str, href: str) -> bool:
        key_l = key.lower()
        href_l = href.lower()
        if any(bad in key_l for bad in FILTER_KEYWORDS):
            return False
        return href_l.endswith((".tif", ".tiff", ".zip")) or ".safe" in href_l

    def _is_valid_href(self, href: str) -> bool:
        if not href:
            return False
        h = href.lower()
        if "this_link" in h:
            return False
        if h.endswith(".html") or "text/html" in h:
            return False
        if not (h.endswith(VALID_EXT) or ".safe" in h or h.startswith("s3://")):
            return False
        return True

    def _normalize_s3(self, href: str) -> str:
        if href.startswith("s3://"):
            p = urlparse(href)
            return f"https://{p.netloc}.s3.amazonaws.com/{p.path.lstrip('/')}"
        return href

    def _safe_filename(self, item_id: str, key: str, href: str, ext: str) -> str:
        base = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{item_id}_{key}")[:48]
        digest = hashlib.md5(f"{item_id}|{key}|{href}".encode("utf-8")).hexdigest()[:8]
        ext = ext if ext else ".dat"
        return f"{base}_{digest}{ext}"

    def download_one(self, session: requests.Session, task: DownloadTask, retry: int, resume: bool) -> bool:
        attempts = retry + 1
        for i in range(attempts):
            try:
                headers = {}
                mode = "wb"
                existing = os.path.getsize(task.output_path) if (resume and os.path.exists(task.output_path)) else 0
                if existing > 0:
                    headers["Range"] = f"bytes={existing}-"
                    mode = "ab"

                with session.get(task.url, timeout=120, stream=True, headers=headers, allow_redirects=True) as r:
                    if r.status_code in (401, 403):
                        self.log(f"认证失败: {task.url} HTTP {r.status_code}")
                        return False
                    r.raise_for_status()
                    if r.status_code == 200 and mode == "ab":
                        mode = "wb"
                    os.makedirs(os.path.dirname(task.output_path), exist_ok=True)
                    with open(task.output_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                return True
            except requests.RequestException as exc:
                if i == attempts - 1:
                    self.log(f"下载失败: {task.url} ({exc})")
            except OSError as exc:
                self.log(f"文件写入失败: {task.output_path} ({exc})")
                return False
        return False
