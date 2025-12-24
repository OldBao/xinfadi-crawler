# 🥬 新发地农产品价格爬虫

自动抓取[北京新发地农产品价格数据](http://www.xinfadi.com.cn/priceDetail.html)，支持导出 CSV/Excel 报表，可同步到飞书电子表格。

## ✨ 功能特点

- 📊 抓取每日价格数据（蔬菜、水果、肉禽蛋、水产、粮油、豆制品、调料）
- 📅 支持日期范围查询
- 🏷️ 支持按分类筛选
- 📁 导出 CSV 或 Excel 格式
- ☁️ 同步到飞书电子表格（支持个人云盘）
- ⏰ 支持定时自动抓取

---

## 🚀 快速开始

### 第一步：安装

```bash
# 进入项目目录
cd /Users/andy.zhanggx/projects/crawler

# 创建虚拟环境（如果还没有）
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 第二步：运行

```bash
# 激活虚拟环境（每次使用前都需要）
source venv/bin/activate

# 抓取今日数据
python xinfadi_crawler.py --today
```

数据会保存到 `./data/` 目录下。

---

## 📖 使用说明

### 基础命令

| 命令 | 说明 |
|------|------|
| `python xinfadi_crawler.py --today` | 抓取今日数据 |
| `python xinfadi_crawler.py --yesterday` | 抓取昨日数据 |
| `python xinfadi_crawler.py --days 7` | 抓取最近7天数据 |

### 日期范围查询

```bash
# 抓取指定日期范围
python xinfadi_crawler.py --start 2025-12-01 --end 2025-12-24

# 只指定开始日期（到今天）
python xinfadi_crawler.py --start 2025-12-01
```

### 分类筛选

支持的分类：`蔬菜`、`水果`、`肉禽蛋`、`水产`、`粮油`、`豆制品`、`调料`

```bash
# 只抓取水果数据
python xinfadi_crawler.py --category 水果 --today

# 抓取最近7天的蔬菜数据
python xinfadi_crawler.py --category 蔬菜 --days 7
```

### 输出格式

```bash
# 导出为 Excel 格式
python xinfadi_crawler.py --today --format xlsx

# 同时导出 CSV 和 Excel
python xinfadi_crawler.py --today --format both
```

---

## ☁️ 飞书同步

支持两种模式：
- **简单模式**：无需浏览器授权，文件创建在应用云空间
- **用户授权模式**：需要浏览器登录授权，文件创建在**个人云盘**

### 第一步：创建飞书应用

1. 访问 [飞书开放平台](https://open.feishu.cn/)
2. 创建企业自建应用
3. 在【安全设置】添加重定向URL：`http://localhost:9000/callback`
4. 在【权限管理】开通权限（**应用身份权限**和**用户身份权限**都要开通）：
   - `sheets:spreadsheet` - 电子表格权限
   - `drive:drive` - 云空间权限
5. **发布应用版本**

### 第二步：配置

```bash
# 初始化配置文件
python feishu_sync.py --init

# 编辑配置文件，填入 app_id 和 app_secret
```

### 第三步：选择授权模式

#### 方式A：简单模式（推荐个人使用）

```bash
python feishu_sync.py --simple
```

无需浏览器授权，直接使用。文件创建在应用关联的云空间。

#### 方式B：用户授权模式（文件在个人云盘）

```bash
python feishu_sync.py --auth
```

会打开浏览器，登录飞书授权。文件创建在你的**个人云盘**。

### 第四步：测试连接

```bash
python feishu_sync.py --test
```

### 第五步：同步数据

```bash
# 抓取今日数据并同步到飞书
python xinfadi_crawler.py --today --sync-feishu

# 定时抓取并自动同步
python xinfadi_crawler.py --schedule --sync-feishu
```

---

## 📁 输出文件

### 本地文件

```
data/
├── xinfadi_price_2025-12-24_to_2025-12-24.csv
├── xinfadi_price_2025-12-24_to_2025-12-24.xlsx
└── ...
```

### 飞书文件

每次同步会创建新的电子表格文件：
- `新发地价格_2025-12-24`
- 如果重名会自动加后缀：`新发地价格_2025-12-24_(1)`

### 数据格式

| 一级分类 | 二级分类 | 品名 | 最低价 | 平均价 | 最高价 | 规格 | 产地 | 单位 | 发布日期 |
|----------|----------|------|--------|--------|--------|------|------|------|----------|
| 蔬菜 | | 大白菜 | 0.8 | 1.0 | 1.2 | | 冀鄂 | 斤 | 2025-12-24 |

---

## ⏰ 定时任务

### 使用内置定时器

```bash
# 每天 8:00 自动抓取并同步
python xinfadi_crawler.py --schedule --sync-feishu

# 自定义时间
python xinfadi_crawler.py --schedule --schedule-time 06:30 --sync-feishu
```

### 使用系统 crontab

```bash
# 编辑定时任务
crontab -e

# 添加（每天 8:00 运行）
0 8 * * * cd /Users/andy.zhanggx/projects/crawler && ./venv/bin/python xinfadi_crawler.py --today --sync-feishu >> ./logs/cron.log 2>&1
```

---

## 🛠️ 完整参数

```bash
python xinfadi_crawler.py --help
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--today` | 抓取今日数据 | - |
| `--yesterday` | 抓取昨日数据 | - |
| `--days N` | 抓取最近N天 | - |
| `--start` | 开始日期 | - |
| `--end` | 结束日期 | - |
| `--category` | 分类筛选 | 全部 |
| `--format` | 输出格式 (csv/xlsx/both) | csv |
| `--sync-feishu` | 同步到飞书 | 否 |
| `--schedule` | 启动定时任务 | - |
| `--schedule-time` | 定时时间 | 08:00 |

---

## ❓ 常见问题

### Q: 飞书授权页面卡住
A: 确保在【权限管理】中开通了**用户身份权限**（不只是应用身份权限）

### Q: 文件在应用空间而不是个人云盘
A: 使用用户授权模式：`python feishu_sync.py --auth`

### Q: Token过期
A: 重新授权：`python feishu_sync.py --auth`

### Q: 提示模块未找到
A: 激活虚拟环境：`source venv/bin/activate`

---

## 📜 许可证

仅供学习研究使用，请遵守网站使用条款。

---

## 🔗 相关链接

- 数据来源：[北京新发地农产品价格](http://www.xinfadi.com.cn/priceDetail.html)
- 飞书开放平台：[https://open.feishu.cn/](https://open.feishu.cn/)
