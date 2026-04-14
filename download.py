#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Callable
from urllib import parse, request
from urllib.error import HTTPError, URLError

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


class SimpleSession:
    def __init__(self, auth_header: str | None = None) -> None:
        self.auth_header = auth_header
        self.ua = "RS-Batch-Downloader/1.4"

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        h = {"User-Agent": self.ua}
        if self.auth_header:
            h["Authorization"] = self.auth_header
        if extra:
            h.update(extra)
        return h

    def get_json(self, url: str, params: dict[str, str] | None = None, timeout: int = 45) -> dict[str, Any]:
        if params:
            url = f"{url}?{parse.urlencode(params)}"
        req = request.Request(url, headers=self._headers())
        with request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def post_json(self, url: str, payload: dict[str, Any], timeout: int = 45) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(url, method="POST", data=body, headers=self._headers({"Content-Type": "application/json"}))
        with request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))


class DownloadEngine:
    def __init__(self, logger: Callable[[str], None]) -> None:
        self.log = logger

    def build_session(self, nasa_user: str, nasa_password: str, nasa_token: str) -> SimpleSession:
        auth_header = None
        if nasa_token.strip():
            auth_header = f"Bearer {nasa_token.strip()}"
            self.log("NASA认证: 使用Token")
        elif nasa_user.strip() and nasa_password:
            raw = f"{nasa_user.strip()}:{nasa_password}".encode("utf-8")
            auth_header = "Basic " + base64.b64encode(raw).decode("ascii")
            self.log("NASA认证: 使用用户名/密码")
        else:
            self.log("NASA认证: 未配置（MODIS可能401/403）")
        return SimpleSession(auth_header)

    def verify_auth(self, session: SimpleSession) -> bool:
        try:
            req = request.Request(EARTHDATA_PROFILE, headers=session._headers())
            with request.urlopen(req, timeout=20) as resp:
                status = getattr(resp, "status", 200)
                ok = status < 400
                self.log(f"NASA认证检测: {'成功' if ok else '失败'} (HTTP {status})")
                return ok
        except (HTTPError, URLError) as exc:
            code = getattr(exc, "code", "ERR")
            self.log(f"NASA认证检测失败: HTTP {code}")
            return False

    def search_dataset(
        self,
        session: SimpleSession,
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
        return self._search_stac(session, dataset, bbox, geometry, start_date, end_date, max_items, cloud)

    def _search_stac(
        self,
        session: SimpleSession,
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
        data = session.post_json(STAC_SEARCH, payload, timeout=45)
        return data.get("features", [])

    def _search_modis_cmr(
        self,
        session: SimpleSession,
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
        data = session.get_json(CMR_GRANULES, params=params, timeout=45)
        entries = data.get("feed", {}).get("entry", [])

        items: list[dict[str, Any]] = []
        for e in entries:
            item_id = e.get("producer_granule_id") or e.get("id") or "modis"
            assets: dict[str, dict[str, str]] = {}
            for idx, link in enumerate(e.get("links", [])):
                href = str(link.get("href", ""))
                if self._is_valid_href(href):
                    assets[f"cmr_{idx}"] = {"href": href}
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
                ext = os.path.splitext(parse.urlparse(href).path)[1].lower()
                if not ext and ".safe" in href.lower():
                    ext = ".safe"
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
                        requires_auth=dataset.startswith("modis-"),
                    )
                )
        return tasks, filtered

    def _select_assets(self, dataset: str, assets: dict[str, Any]) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        if dataset == "sentinel-1-grd":
            for preferred in ["data", "product"]:
                href = self._asset_href(assets.get(preferred))
                if href and self._is_imagery_asset(preferred, href):
                    pairs.append((preferred, href))
            if pairs:
                return pairs

        for key, meta in assets.items():
            key_l = str(key).lower()
            if any(bad in key_l for bad in FILTER_KEYWORDS):
                continue
            href = self._asset_href(meta)
            if not href:
                continue
            if dataset == "sentinel-1-grd" and not self._is_imagery_asset(str(key), href):
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
            p = parse.urlparse(href)
            return f"https://{p.netloc}.s3.amazonaws.com/{p.path.lstrip('/')}"
        return href

    def _safe_filename(self, item_id: str, key: str, href: str, ext: str) -> str:
        base = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{item_id}_{key}")[:48]
        digest = hashlib.md5(f"{item_id}|{key}|{href}".encode("utf-8")).hexdigest()[:8]
        ext = ext if ext else ".dat"
        return f"{base}_{digest}{ext}"

    def download_one(self, session: SimpleSession, task: DownloadTask, retry: int, resume: bool) -> bool:
        attempts = retry + 1
        for i in range(attempts):
            try:
                existing = os.path.getsize(task.output_path) if (resume and os.path.exists(task.output_path)) else 0
                headers: dict[str, str] = {}
                mode = "wb"
                if existing > 0:
                    headers["Range"] = f"bytes={existing}-"
                    mode = "ab"

                req = request.Request(task.url, headers=session._headers(headers))
                with request.urlopen(req, timeout=120) as resp:
                    status = getattr(resp, "status", 200)
                    if status in (401, 403):
                        self.log(f"认证失败: {task.url} HTTP {status}")
                        return False
                    if status == 200 and mode == "ab":
                        mode = "wb"
                    os.makedirs(os.path.dirname(task.output_path), exist_ok=True)
                    with open(task.output_path, mode) as f:
                        while True:
                            chunk = resp.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                return True
            except HTTPError as exc:
                if exc.code in (401, 403):
                    self.log(f"认证失败: {task.url} HTTP {exc.code}")
                    return False
                if i == attempts - 1:
                    self.log(f"下载失败: {task.url} ({exc})")
            except (URLError, OSError) as exc:
                if i == attempts - 1:
                    self.log(f"下载失败: {task.url} ({exc})")
        return False
