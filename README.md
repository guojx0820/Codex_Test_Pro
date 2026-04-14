# 遥感数据批量下载工具（免注册优先）

这是一个基于 **Python + Tkinter** 的可视化桌面工具，目标是：

- 不依赖复杂开发环境；
- 尽量使用公开可访问的数据接口（优先免注册）；
- 通过输入时间范围与区域范围（BBox）即可批量检索并下载遥感数据。

## 支持数据源（当前版本）

当前通过公开 STAC API（`earth-search`）检索，默认支持：

- Sentinel-2 L2A (`sentinel-2-l2a`)
- Landsat Collection 2 L2 (`landsat-c2-l2`)
- Sentinel-1 GRD (`sentinel-1-grd`)

> 说明：不同地区和时间段，数据可用性不同；部分场景下载速度受网络条件影响。

## 一键运行（Windows）

双击：

- `一键启动.bat`

该脚本会自动调用跨平台启动器（`launcher.py`）：

1. 检查 Python；
2. 自动创建（或复用）虚拟环境；
3. 启动图形界面；
4. 失败时显示明确错误，不再“闪退”。

## 一键运行（Linux/macOS）

```bash
chmod +x run.sh
./run.sh
```

## 软件使用流程

1. 选择数据集（可多选）；
2. 输入时间（`YYYY-MM-DD`）；
3. 输入区域 BBox：`min_lon,min_lat,max_lon,max_lat`；
4. 选择输出目录；
5. 点击“开始批量下载”。

## 免门槛说明

- 软件本体使用标准库（`tkinter`、`urllib` 等），尽量减少依赖；
- 已提供可直接双击的启动脚本，降低使用门槛；
- 如需进一步“安装即用”（无需 Python），可用 `build_exe.bat` 生成单文件 exe。

> 注意：新版 `build_exe.bat` 不再自动联网安装依赖，避免代理/网络问题导致失败。

## 打包 EXE（可选）

双击：

- `build_exe.bat`

完成后可在 `dist/` 下获得可分发程序（Windows）。

---

如果你希望，我可以继续在下一版加上：

- 行政区/矢量边界输入（GeoJSON/Shapefile）；
- 断点续传与失败重试；
- 按卫星传感器预设波段批量下载；
- 任务队列与历史任务管理。


## v1.2 新增能力

- 修复 Landsat/Sentinel-1 的 `s3://` 下载失败问题：自动转换为公开 `https://bucket.s3.amazonaws.com/...` 链接。
- 增加断点续传（Range）与失败自动重试。
- 增加矢量边界输入：支持 BBox / GeoJSON / Shapefile（Shapefile 需安装 `pyshp`）。
- 增加按传感器预设波段批量下载（每景可设置资产数量）。
- 增加任务历史记录（`task_history.json`）和“查看历史任务”。
- 新增数据集选项：`modis-13q1-061`、`modis-09a1-061`、`goci-l2`（实际可用性受上游 STAC 服务支持情况影响）。

## 注意

- 若下载任务超时，程序会自动重试；网络不稳定时可适当减少“每数据集最多景数”并增大重试次数。
- 如果选择 Shapefile 输入，请先执行：`pip install pyshp`。


## v1.3 关键修复

- **修复 Sentinel-1/Landsat 在 Windows 可能出现的路径过长导致的 `[Errno 2]` 失败**：文件名自动哈希缩短。
- **提升下载速度**：新增并发下载线程参数（默认4线程）。
- **MODIS/GOCI 改为 CMR 数据源检索**，不再依赖 earth-search 的集合名。
- **CMR 结果自动提取 granule 数据链接**，支持批量下载。

建议：在 Windows 中将输出目录设置为较短路径（如 `D:\RS`）以获得更高稳定性。


## v1.4 认证与资产过滤修复

- 新增 `download.py` 下载引擎，按数据源分流：
  - MODIS (`modis-13q1-061`, `modis-09a1-061`) 走 **NASA CMR API + Earthdata认证**；
  - 其他数据集走 STAC API。
- GUI 新增 NASA 认证输入：用户名、密码、Token（MODIS 推荐填写）。
- Sentinel-1 资产选择改为：优先 `data` / `product`，并过滤 `schema/metadata/thumbnail/overview`。
- 下载链接严格过滤：
  - 仅保留 `.hdf/.tif/.tiff/.zip/.SAFE`；
  - 丢弃 `This_link...` 和 HTML 链接。
- 日志新增：
  - 认证检测结果；
  - 过滤资产数量；
  - 实际下载文件类型。

> 运行依赖：无需额外第三方库，默认使用 Python 标准库网络模块。


## v1.6 新增

- 新增“下载前链路预检”按钮：可先探测可下载数量、需认证数量、预计失败链接数量，再决定是否启动批量任务。


## v1.6.1 修复

- 修复 `name 'urlparse' is not defined` 异常。
- 修复 Landsat/Sentinel-1 资产 `href` 为 `s3://` 时被误判为空链接的问题（自动协议转换，不拼接新路径）。
- 修复 MODIS CMR 检索 401：检索阶段不附带认证头，仅下载阶段使用认证会话。
