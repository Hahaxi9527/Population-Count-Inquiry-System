import tkinter as tk
from tkinter import simpledialog, messagebox
from PIL import Image, ImageTk
import requests
import json
import matplotlib.pyplot as plt
from matplotlib import rcParams
import numpy as np

# 配置 Matplotlib 支持中文字体
rcParams['font.sans-serif'] = ['SimHei']  # 使用 SimHei（黑体）字体
rcParams['axes.unicode_minus'] = False   # 解决负号显示问题

# 定义服务器地址
SERVER_URL = "http://localhost:8000/query"

class PopulationQueryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("人口查询系统")

        # 用户绘制的点和多边形
        self.points = []  # 画布坐标点
        self.geo_points = []  # 经纬度点

        # 经纬度范围 (基于地图图片定义)
        self.lon_min, self.lon_max = -180, 180
        self.lat_min, self.lat_max = -90, 90

        # 初始化画布
        self.canvas_width = 800
        self.canvas_height = 400
        self.canvas = tk.Canvas(root, bg="white", width=self.canvas_width, height=self.canvas_height)
        self.canvas.pack(side="left", fill="both", expand=False)

        # 添加右侧按钮和列表框
        self.control_frame = tk.Frame(root)
        self.control_frame.pack(side="right", fill="y", padx=10)

        self.add_lat_lon_button = tk.Button(self.control_frame, text="手动添加经纬度", command=self.add_lat_lon)
        self.add_lat_lon_button.pack(pady=5)

        self.submit_button = tk.Button(self.control_frame, text="提交多边形", command=self.submit_polygon)
        self.submit_button.pack(pady=5)

        self.clear_button = tk.Button(self.control_frame, text="清除多边形", command=self.clear_polygon)
        self.clear_button.pack(pady=5)

        # 显示选取点的经纬度
        self.points_label = tk.Label(self.control_frame, text="选取的点：")
        self.points_label.pack(pady=5)

        self.points_listbox = tk.Listbox(self.control_frame, height=20, width=40)
        self.points_listbox.pack(pady=5)

        # 文本框用于显示总人口数
        self.population_label = tk.Label(self.control_frame, text="总人口: 未查询", font=("Arial", 12), fg="blue")
        self.population_label.pack(pady=5)

        # 加载地图图片
        self.load_static_image("resources/img_1.png")

        # 绑定鼠标点击事件
        self.canvas.bind("<Button-1>", self.add_point)

    def load_static_image(self, image_path):
        try:
            self.image = Image.open(image_path)

            # 保持图片宽高比，调整以适配画布
            img_ratio = self.image.width / self.image.height
            canvas_ratio = self.canvas_width / self.canvas_height

            if img_ratio > canvas_ratio:
                new_width = self.canvas_width
                new_height = int(self.canvas_width / img_ratio)
            else:
                new_height = self.canvas_height
                new_width = int(self.canvas_height * img_ratio)

            self.image = self.image.resize((new_width, new_height), Image.LANCZOS)
            self.tk_image = ImageTk.PhotoImage(self.image)
            self.image_width = new_width
            self.image_height = new_height

            self.canvas.create_image(0, 0, anchor="nw", image=self.tk_image)
            self.canvas.config(width=new_width, height=new_height)
        except Exception as e:
            messagebox.showerror("错误", f"加载地图失败: {e}")

    def add_point(self, event):
        if event.x < 0 or event.x > self.image_width or event.y < 0 or event.y > self.image_height:
            return

        x, y = event.x, event.y
        self.points.append((x, y))
        self.canvas.create_oval(x - 2, y - 2, x + 2, y + 2, fill="red")

        lon = self.lon_min + (self.lon_max - self.lon_min) * x / self.image_width
        lat = self.lat_max - (self.lat_max - self.lat_min) * y / self.image_height

        # 控制经纬度精度
        lon = round(lon, 5)
        lat = round(lat, 5)

        self.geo_points.append((lon, lat))
        self.update_points_list()

        if len(self.points) > 1:
            self.canvas.create_line(self.points[-2][0], self.points[-2][1], x, y, fill="blue")

    def add_lat_lon(self):
        try:
            lon = float(simpledialog.askstring("输入经度", "请输入经度 (-180 ~ 180):"))
            lat = float(simpledialog.askstring("输入纬度", "请输入纬度 (-90 ~ 90):"))

            # 控制经纬度精度
            lon = round(lon, 5)
            lat = round(lat, 5)

            x = int((lon - self.lon_min) / (self.lon_max - self.lon_min) * self.image_width)
            y = int((self.lat_max - lat) / (self.lat_max - self.lat_min) * self.image_height)

            self.points.append((x, y))
            self.geo_points.append((lon, lat))
            self.canvas.create_oval(x - 2, y - 2, x + 2, y + 2, fill="red")
            self.update_points_list()

            if len(self.points) > 1:
                self.canvas.create_line(self.points[-2][0], self.points[-2][1], x, y, fill="blue")
        except ValueError:
            messagebox.showerror("错误", "请输入有效的经纬度！")

    def clear_polygon(self):
        self.points = []
        self.geo_points = []
        self.canvas.delete("all")
        self.canvas.create_image(0, 0, anchor="nw", image=self.tk_image)
        self.points_listbox.delete(0, tk.END)

    def submit_polygon(self):
        if len(self.geo_points) < 3:
            messagebox.showerror("错误", "多边形至少需要 3 个点！")
            return

        # 闭合多边形
        if self.geo_points[0] != self.geo_points[-1]:
            self.geo_points.append(self.geo_points[0])  # 闭合多边形
            self.points.append(self.points[0])

            self.canvas.create_line(self.points[-2][0], self.points[-2][1], self.points[-1][0], self.points[-1][1],
                                    fill="blue")
            self.update_points_list()

        # 检查是否为凸多边形
        if not self.is_convex_polygon(self.geo_points):
            messagebox.showerror("错误", "多边形不是凸多边形，请重新绘制！")
            self.clear_polygon()  # 清空当前绘制的多边形
            return

        formatted_geo_points = [
            [round(lon, 5), round(lat, 5)] for lon, lat in self.geo_points
        ]
        polygon_geojson = {
            "type": "Polygon",
            "coordinates": [formatted_geo_points]
        }

        # 打印调试，确保 GeoJSON 格式正确
        print("Submitting GeoJSON to server:")
        print(json.dumps(polygon_geojson, indent=4))

        headers = {'Content-Type': 'application/json'}
        try:
            response = requests.post(SERVER_URL, headers=headers, data=json.dumps(polygon_geojson))
            if response.status_code == 200:
                result = response.json()
                self.display_result(result)
            else:
                messagebox.showerror("错误", f"服务器错误: {response.status_code}")
        except requests.exceptions.RequestException as e:
            messagebox.showerror("错误", f"请求失败: {e}")

    def is_convex_polygon(self, points):
        """
        检查多边形是否为凸多边形
        :param points: 多边形顶点列表 (经纬度列表)
        :return: 如果是凸多边形返回 True，否则返回 False
        """

        def cross_product(p1, p2, p3):
            """
            计算向量 (p2 - p1) 和 (p3 - p2) 的叉积
            :param p1: 点1
            :param p2: 点2
            :param p3: 点3
            :return: 叉积结果
            """
            x1, y1 = p2[0] - p1[0], p2[1] - p1[1]
            x2, y2 = p3[0] - p2[0], p3[1] - p2[1]
            return x1 * y2 - y1 * x2

        n = len(points)
        if n < 4:  # 3个点或者少于3个点的多边形总是凸的
            return True

        # 记录第一个非零叉积的方向
        prev_cross_product = None
        for i in range(n):
            p1, p2, p3 = points[i], points[(i + 1) % n], points[(i + 2) % n]
            cross = cross_product(p1, p2, p3)
            if cross != 0:
                if prev_cross_product is None:
                    prev_cross_product = cross  # 初始化方向
                elif cross * prev_cross_product < 0:  # 检查方向是否一致
                    return False

        return True

    def update_points_list(self):
        self.points_listbox.delete(0, tk.END)
        for lon, lat in self.geo_points:
            self.points_listbox.insert(tk.END, f"经度: {lon:.5f}, 纬度: {lat:.5f}")

    def display_result(self, result):
        total_population = result.get("total_population", 0)
        cells = result.get("cells", [])

        # 更新总人口数到文本框
        self.population_label.config(text=f"总人口: {total_population}")

        # 显示总人口和单元格数量
        messagebox.showinfo("查询结果", f"总人口: {total_population}\n单元格数量: {len(cells)}")

        if cells:
            # 提取经纬度和人口数据
            lons = [cell['longitude'] for cell in cells]
            lats = [cell['latitude'] for cell in cells]
            pops = [cell['population'] for cell in cells]

            # 创建网格数据，确定网格分辨率
            lon_min, lon_max = min(lons), max(lons)
            lat_min, lat_max = min(lats), max(lats)
            grid_resolution = 0.01  # 每个网格的大小（经纬度）

            # 构建网格坐标
            lon_bins = np.arange(lon_min, lon_max + grid_resolution, grid_resolution)
            lat_bins = np.arange(lat_min, lat_max + grid_resolution, grid_resolution)
            density_grid = np.zeros((len(lat_bins) - 1, len(lon_bins) - 1))

            # 填充网格数据
            for lon, lat, pop in zip(lons, lats, pops):
                lon_idx = np.digitize(lon, lon_bins) - 1
                lat_idx = np.digitize(lat, lat_bins) - 1

                if 0 <= lon_idx < density_grid.shape[1] and 0 <= lat_idx < density_grid.shape[0]:
                    density_grid[lat_idx, lon_idx] += pop

            # 设置颜色条范围，避免极值干扰
            vmin = 0  # 最小值
            vmax = 100
            #vmax = np.nanpercentile(density_grid, 95)  # 使用 95% 分位数作为最大值

            # 绘制网格化人口密度图
            plt.figure(figsize=(12, 8))
            plt.pcolormesh(
                lon_bins,
                lat_bins,
                density_grid,
                cmap='YlOrRd',
                shading='auto',
                vmin=vmin,
                vmax=vmax
            )
            plt.colorbar(label='人口数量')
            plt.xlabel('经度')
            plt.ylabel('纬度')
            plt.title(f'查询区域总人口: {total_population}')
            plt.show()

        else:
            # 如果查询区域没有人口数据
            messagebox.showinfo("结果", "所选区域内没有人口数据。")


if __name__ == "__main__":
    root = tk.Tk()
    app = PopulationQueryApp(root)
    root.mainloop()
