import os
import numpy as np
from sanic import Sanic, json
from sanic.log import logger
from shapely.geometry import shape, Polygon
import asyncio
import json as py_json

app = Sanic("PopulationQueryService")

# 全局变量，用于记录是否所有文件已处理完成
processing_complete = False

# 解析 .asc 文件并返回网格数据
def parse_asc_file(file_path):
    with open(file_path, 'r') as f:
        header = {}
        for _ in range(6):  # 读取头部信息
            line = f.readline()
            key, value = line.strip().split()
            header[key.lower()] = float(value)

        ncols = int(header['ncols'])
        nrows = int(header['nrows'])
        nodata = header['nodata_value']

        data = np.genfromtxt(f, dtype=np.float32)
        data = data.reshape((nrows, ncols))
        data[data == nodata] = np.nan  # 替换无效值为 NaN

    return header, data

# 将网格数据分块并存储为二进制文件
def save_binary_chunks(file_path, header, data, output_dir="./binary_chunks"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    step = 10  # 每个块大小为 10°×10°
    factor = int(1 / header["cellsize"])  # 每度多少格子
    ncols = int(header['ncols'])
    nrows = int(header['nrows'])
    xllcorner = header['xllcorner']
    yllcorner = header['yllcorner']

    total_chunks = (ncols // (step * factor)) * (nrows // (step * factor))
    processed_chunks = 0

    for lon_offset in range(0, ncols, step * factor):
        for lat_offset in range(0, nrows, step * factor):
            chunk = data[lat_offset: lat_offset + step * factor,
                         lon_offset: lon_offset + step * factor]
            if np.isnan(chunk).all():
                continue

            lon = xllcorner + lon_offset * header["cellsize"]
            lat = yllcorner + (nrows - (lat_offset + step * factor)) * header["cellsize"]

            chunk_file_name = f"lon_{int(lon)}_lat_{int(lat)}.npy"
            chunk_file_path = os.path.join(output_dir, chunk_file_name)
            np.save(chunk_file_path, chunk)

            # 输出当前分块处理的进度
            processed_chunks += 1
            logger.info(f"已处理 {processed_chunks}/{total_chunks} 个数据块")

    logger.info(f"完成解析并分块存储: {file_path}")

# 同步文件预处理，解析所有 .asc 文件并保存为二进制块
def preprocess_files_sync(input_dir, output_dir="./binary_chunks"):
    asc_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".asc")]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logger.info(f"需要处理的 .asc 文件数量：{len(asc_files)}")
    total_files = len(asc_files)
    processed_files = 0

    for file_path in asc_files:
        logger.info(f"正在解析文件: {file_path}")
        header, data = parse_asc_file(file_path)
        save_binary_chunks(file_path, header, data, output_dir)

        # 输出处理的文件进度
        processed_files += 1
        logger.info(f"已处理 {processed_files}/{total_files} 个文件")

    logger.info("所有文件预处理完成！")

# 异步执行文件预处理
async def preprocess_files_async():
    global processing_complete
    binary_dir = "./binary_chunks"
    input_dir = "./resources/gpw-v4-population-count-rev11_2020_30_sec_asc"

    if not os.path.exists(binary_dir) or not os.listdir(binary_dir):
        logger.info("未找到二进制数据块，开始预处理...")
        preprocess_files_sync(input_dir, binary_dir)

    processing_complete = True
    logger.info("所有文件已处理完成！")

# 服务启动后预处理文件
@app.listener("after_server_start")
async def start_preprocessing(app, loop):
    asyncio.create_task(preprocess_files_async())

@app.route("/query", methods=["POST"])
async def query_population(request):
    global processing_complete

    # 检查数据是否已准备好
    if not processing_complete:
        return json({"message": "数据正在加载，请稍后重试"}, status=503)

    # 解析GeoJSON请求
    geojson = request.json
    if not geojson:
        return json({"error": "Invalid GeoJSON input."}, status=400)

    try:
        polygon = shape(geojson)
    except Exception as e:
        return json({"error": f"GeoJSON解析失败: {e}"}, status=400)

    if not isinstance(polygon, Polygon) or not polygon.is_valid:
        return json({"error": "Input must be a valid GeoJSON Polygon."}, status=400)

    # 获取所有二进制块文件
    binary_dir = "./binary_chunks"
    if not os.path.exists(binary_dir):
        return json({"error": "没有找到预处理的二进制数据块！"}, status=500)

    chunk_files = [os.path.join(binary_dir, f) for f in os.listdir(binary_dir) if f.endswith(".npy")]
    logger.info(f"总计找到 {len(chunk_files)} 个数据块进行查询。")

    # 获取多边形的边界范围
    poly_bounds = polygon.bounds  # (min_lon, min_lat, max_lon, max_lat)
    poly_min_lon, poly_min_lat, poly_max_lon, poly_max_lat = poly_bounds
    logger.info(f"多边形边界范围: lon_min={poly_min_lon}, lon_max={poly_max_lon}, "
                f"lat_min={poly_min_lat}, lat_max={poly_max_lat}")

    total_population = 0.0
    cells = []

    # 遍历所有块文件
    for chunk_index, chunk_file in enumerate(chunk_files):
        logger.info(f"正在处理第 {chunk_index + 1}/{len(chunk_files)} 个数据块: {chunk_file}")

        # 从文件名中提取块的经纬度范围
        file_name = os.path.basename(chunk_file)
        parts = file_name.replace("lon_", "").replace("lat_", "").replace(".npy", "").split("_")
        lon_min = float(parts[0])
        lat_min = float(parts[1])
        lon_max = lon_min + 10
        lat_max = lat_min + 10

        logger.info(f"数据块范围: lon_min={lon_min}, lon_max={lon_max}, lat_min={lat_min}, lat_max={lat_max}")

        # 快速过滤与多边形不相交的块
        if lon_max <= poly_min_lon or lon_min >= poly_max_lon or lat_max <= poly_min_lat or lat_min >= poly_max_lat:
            logger.info(f"跳过与多边形无交集的数据块: {chunk_file}")
            continue

        # 检查是否整个块都在多边形内
        chunk_bounds_polygon = Polygon([
            (lon_min, lat_min),
            (lon_min, lat_max),
            (lon_max, lat_max),
            (lon_max, lat_min),
        ])
        if polygon.contains(chunk_bounds_polygon):
            logger.info(f"整个数据块完全包含在多边形内: {chunk_file}")
            chunk = np.load(chunk_file)
            if chunk is not None and chunk.size > 0:
                total_population += float(np.nansum(chunk))  # 快速计算整个块的人口总和
            continue

        # 延迟加载数据块，只处理部分相交的块
        logger.info(f"部分相交的数据块: {chunk_file}")
        chunk = np.load(chunk_file)
        if chunk is None or chunk.size == 0:
            logger.info(f"跳过空数据块: {chunk_file}")
            continue

        # 逐个网格检查
        factor = 3600  # 每度划分 3600 个单元格
        for i in range(chunk.shape[0]):
            for j in range(chunk.shape[1]):
                cell_value = chunk[i, j]
                if np.isnan(cell_value):
                    continue

                # 计算网格的经纬度范围
                x_min = lon_min + j * (1 / factor)
                y_min = lat_min + i * (1 / factor)
                cell_polygon = Polygon([
                    (x_min, y_min),
                    (x_min + 1 / factor, y_min),
                    (x_min + 1 / factor, y_min + 1 / factor),
                    (x_min, y_min + 1 / factor),
                ])

                # 检查网格与多边形的关系
                if polygon.contains(cell_polygon):
                    total_population += float(cell_value)
                    cells.append({
                        "longitude": round(float(x_min), 5),
                        "latitude": round(float(y_min), 5),
                        "population": round(float(cell_value))
                    })
                elif polygon.intersects(cell_polygon):
                    inter_area = polygon.intersection(cell_polygon).area
                    cell_area = (1 / factor) ** 2
                    population = float(cell_value) * (inter_area / cell_area)
                    total_population += population
                    cells.append({
                        "longitude": round(float(x_min), 5),
                        "latitude": round(float(y_min), 5),
                        "population": round(float(population))
                    })

    # 转换为标准Python数据类型
    total_population = float(total_population)  # 确保是标准 float 类型
    # 确保 cells 数据格式正确
    cells = [
        {
            "longitude": float(cell["longitude"]),
            "latitude": float(cell["latitude"]),
            "population": int(cell["population"])  # 转为整数
        }
        for cell in cells
    ]

    # 写入文件前确保旧文件被删除
    output_file_path = "output_cells.json"
    try:
        # 检查文件是否存在，如果存在则删除
        if os.path.exists(output_file_path):
            os.remove(output_file_path)

        # 写入新的数据
        with open(output_file_path, "w", encoding="utf-8") as f:
            py_json.dump(cells, f, ensure_ascii=False, indent=4)
        logger.info(f"单元格详细信息已成功输出到文件: {output_file_path}")

    except Exception as e:
        logger.error(f"输出单元格信息到文件时发生错误: {e}")

    logger.info(f"查询结果: 总人口={int(total_population)}, 单元格数量={len(cells)}")

    return json({
        "total_population": int(total_population),  # 确保返回整数总人口
        "cells": cells  # 返回完整单元格数据
    })

# 启动服务
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
