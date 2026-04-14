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

该脚本会自动：

1. 检查 Python；
2. 自动创建虚拟环境；
3. 启动图形界面。

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
- 如需进一步“安装即用”（无需 Python），可用 `build_exe.bat` 生成单文件 exe（需联网自动安装 PyInstaller）。

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
