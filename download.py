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


class SessionResponse:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self._body = body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(url="", code=self.status_code, msg=f"HTTP {self.status_code}", hdrs=None, fp=None)

    def iter_content(self, chunk_size: int):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]


class SimpleSession:
    def __init__(self, auth_header: str | None = None) -> None:
        self.auth_header = auth_header
        self.user_agent = "RS-Batch-Downloader/1.6"

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"User-Agent": self.user_agent}
        if self.auth_header:
            headers["Authorization"] = self.auth_header
        if extra:
            headers.update(extra)
        return headers

    def get(self, url: str, params: dict[str, str] | None = None, timeout: int = 45, headers: dict[str, str] | None = None, add_auth: bool = True) -> SessionResponse:
        if params:
            q = parse.urlencode(params)
            url = f"{url}?{q}"
        base_headers = self._headers(headers) if add_auth else {"User-Agent": self.user_agent, **(headers or {})}
        req = request.Request(url, headers=base_headers)
        with request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            body = resp.read()
            return SessionResponse(status, body)

    def post_json(self, url: str, payload: dict[str, Any], timeout: int = 45) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=data,
            method="POST",
            headers=self._headers({"Content-Type": "application/json"}),
        )
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
            resp = session.get(EARTHDATA_PROFILE, timeout=20)
            ok = resp.status_code < 400
            self.log(f"NASA认证检测: {'成功' if ok else '失败'} (HTTP {resp.status_code})")
            return ok
        except (HTTPError, URLError) as exc:
            code = getattr(exc, "code", "ERR")
            self.log(f"NASA认证检测异常: HTTP {code}")
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
        if dataset.startswith("modis"):
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
        resp = session.get(CMR_GRANULES, params=params, timeout=45, add_auth=False)
        resp.raise_for_status()
        entries = json.loads(resp._body.decode("utf-8")).get("feed", {}).get("entry", [])
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

                if dataset == "sentinel-1-grd" and not href_l.endswith((".tiff", ".tif", ".zip", ".safe")):
                    self.log(f"过滤资产: {asset_key} | href={href} | 原因=非影像")
                    filtered += 1
                    continue

                ext = self._ext_from_href(href_l)
                if not ext:
                    self.log(f"过滤资产: {asset_key} | href={href} | 原因=未知扩展")
                    filtered += 1
                    continue

                filename = self._safe_filename(item_id, str(asset_key), href, ext)
                tasks.append(
                    DownloadTask(
                        dataset=dataset,
                        item_id=item_id,
                        asset_key=str(asset_key),
                        url=href,
                        output_path=os.path.join(ds_dir, filename),
                        file_type=ext,
                        requires_auth=dataset.startswith("modis"),
                    )
                )
                selected += 1
                if selected >= asset_limit:
                    break

        return tasks, filtered

    def precheck_tasks(self, session: SimpleSession, tasks: list[DownloadTask], max_checks: int = 20) -> dict[str, int]:
        ok = 0
        auth = 0
        bad = 0
        for task in tasks[:max_checks]:
            try:
                req = request.Request(task.url, headers=session._headers({"Range": "bytes=0-1023"}))
                with request.urlopen(req, timeout=20) as resp:
                    status = getattr(resp, "status", 200)
                    if status in (200, 206):
                        ok += 1
                    elif status in (401, 403):
                        auth += 1
                    else:
                        bad += 1
            except HTTPError as exc:
                if exc.code in (401, 403):
                    auth += 1
                else:
                    bad += 1
            except URLError:
                bad += 1
        return {"ok": ok, "auth": auth, "bad": bad, "checked": min(len(tasks), max_checks)}


    def _s3_to_https(self, href: str) -> str:
        # 仅把 STAC 原始 s3 href 做协议转换，不拼接新路径
        if not href.startswith("s3://"):
            return href
        parsed = parse.urlparse(href)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        region_buckets = {"usgs-landsat": "us-west-2", "sentinel-s1-l1c": "us-west-2"}
        region = region_buckets.get(bucket)
        if region:
            return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
        return f"https://{bucket}.s3.amazonaws.com/{key}"

    def _asset_href(self, asset: Any) -> str | None:
        if not isinstance(asset, dict):
            return None

        alt = asset.get("alternate", {})
        if isinstance(alt, dict):
            https_node = alt.get("https")
            if isinstance(https_node, dict):
                href = https_node.get("href")
                if isinstance(href, str) and href.startswith("http"):
                    return href
            s3_node = alt.get("s3")
            if isinstance(s3_node, dict):
                href = s3_node.get("href")
                if isinstance(href, str) and href.startswith("s3://"):
                    return self._s3_to_https(href)

        href = asset.get("href")
        if isinstance(href, str):
            if href.startswith("http"):
                return href
            if href.startswith("s3://"):
                return self._s3_to_https(href)
        return None

    def _ext_from_href(self, href_l: str) -> str:
        path = parse.urlparse(href_l).path.lower()
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

    def download_one(self, session: SimpleSession, task: DownloadTask, retry: int, resume: bool) -> bool:
        attempts = retry + 1
        for i in range(attempts):
            try:
                if task.dataset.startswith("modis"):
                    return self._download_with_session(session, task.url, task.output_path)
                return self._download_with_urllib(session, task.url, task.output_path, resume)
            except (HTTPError, URLError, OSError) as exc:
                if i == attempts - 1:
                    self.log(f"下载失败: {task.url} ({exc})")
        return False

    def _download_with_session(self, session: SimpleSession, url: str, path: str) -> bool:
        # MODIS: 必须用 session.get 走认证头
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return True

    def _download_with_urllib(self, session: SimpleSession, url: str, path: str, resume: bool) -> bool:
        existing = os.path.getsize(path) if (resume and os.path.exists(path)) else 0
        headers = {"User-Agent": session.user_agent}
        mode = "wb"
        if existing > 0:
            headers["Range"] = f"bytes={existing}-"
            mode = "ab"

        req = request.Request(url, headers=session._headers(headers))
        with request.urlopen(req, timeout=120) as resp:
            status = getattr(resp, "status", 200)
            if status == 200 and mode == "ab":
                mode = "wb"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, mode) as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
        return True
