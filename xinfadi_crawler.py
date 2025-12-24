#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新发地农产品价格数据爬虫
网站: http://www.xinfadi.com.cn/priceDetail.html

支持功能:
- 抓取指定日期范围的价格数据
- 按分类筛选数据
- 导出CSV/Excel报表
- 定时自动抓取
- 同步到腾讯文档（可选）
"""

import requests
import pandas as pd
import time
import os
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# 飞书同步模块（可选）
try:
    from feishu_sync import load_sync_client, FeishuSync
    FEISHU_AVAILABLE = True
except ImportError:
    FEISHU_AVAILABLE = False


class XinfadiCrawler:
    """新发地价格数据爬虫"""
    
    # API接口地址
    BASE_URL = "http://www.xinfadi.com.cn"
    PRICE_API = f"{BASE_URL}/getPriceData.html"
    
    # 一级分类映射
    CATEGORY_MAP = {
        "蔬菜": 1,
        "水果": 2,
        "肉禽蛋": 3,
        "水产": 4,
        "粮油": 5,
        "豆制品": 6,
        "调料": 7,
    }
    
    # 表头映射
    COLUMNS = [
        "一级分类", "二级分类", "品名", "最低价", 
        "平均价", "最高价", "规格", "产地", "单位", "发布日期"
    ]
    
    def __init__(self, output_dir: str = "./data"):
        """
        初始化爬虫
        
        Args:
            output_dir: 数据输出目录
        """
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": f"{self.BASE_URL}/priceDetail.html",
            "X-Requested-With": "XMLHttpRequest",
        })
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
    
    def fetch_page(
        self,
        page: int = 1,
        limit: int = 20,
        category: Optional[str] = None,
        product_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取单页数据
        
        Args:
            page: 页码
            limit: 每页条数
            category: 分类名称 (蔬菜/水果/肉禽蛋/水产/粮油/豆制品/调料)
            product_name: 产品名称
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            API响应数据
        """
        params = {
            "limit": limit,
            "current": page,
        }
        
        # 添加日期筛选
        if start_date:
            params["pubDateStartTime"] = start_date
        if end_date:
            params["pubDateEndTime"] = end_date
        
        # 添加分类筛选 (使用 prodCat 参数)
        if category:
            params["prodCat"] = category
        
        if product_name:
            params["prodName"] = product_name
        
        try:
            response = self.session.get(self.PRICE_API, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return {"list": [], "total": 0}
        except ValueError as e:
            print(f"JSON解析失败: {e}")
            return {"list": [], "total": 0}
    
    def fetch_all_data(
        self,
        category: Optional[str] = None,
        product_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit_per_page: int = 100,
        max_pages: Optional[int] = None,
        delay: float = 0.5,
    ) -> List[Dict]:
        """
        获取所有数据（自动翻页）
        
        Args:
            category: 分类名称 (蔬菜/水果/肉禽蛋/水产/粮油/豆制品/调料)
            product_name: 产品名称
            start_date: 开始日期
            end_date: 结束日期
            limit_per_page: 每页条数
            max_pages: 最大页数限制
            delay: 请求间隔(秒)
            
        Returns:
            所有数据列表
        """
        all_data = []
        page = 1
        
        print(f"开始抓取数据...")
        if start_date or end_date:
            print(f"日期范围: {start_date or '不限'} 至 {end_date or '不限'}")
        if category:
            print(f"分类: {category}")
        
        while True:
            if max_pages and page > max_pages:
                print(f"已达到最大页数限制: {max_pages}")
                break
            
            result = self.fetch_page(
                page=page,
                limit=limit_per_page,
                category=category,
                product_name=product_name,
                start_date=start_date,
                end_date=end_date,
            )
            
            data_list = result.get("list", [])
            total = result.get("count", 0) or result.get("total", 0)
            
            if not data_list:
                break
            
            all_data.extend(data_list)
            print(f"第 {page} 页: 获取 {len(data_list)} 条数据 (总计: {len(all_data)}/{total})")
            
            # 检查是否还有更多页
            if len(all_data) >= total:
                break
            
            page += 1
            time.sleep(delay)  # 请求间隔，避免给服务器造成压力
        
        print(f"抓取完成! 共获取 {len(all_data)} 条数据")
        return all_data
    
    def parse_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        解析原始数据为DataFrame
        
        Args:
            raw_data: 原始API数据
            
        Returns:
            整理后的DataFrame
        """
        if not raw_data:
            return pd.DataFrame(columns=self.COLUMNS)
        
        parsed = []
        for item in raw_data:
            # prodCat 是分类名称, prodPcat 通常为空
            category = item.get("prodCat", "") or item.get("prodPcat", "")
            parsed.append({
                "一级分类": category,
                "二级分类": item.get("prodPcat", "") if item.get("prodPcat") else "",
                "品名": item.get("prodName", ""),
                "最低价": item.get("lowPrice", ""),
                "平均价": item.get("avgPrice", ""),
                "最高价": item.get("highPrice", ""),
                "规格": item.get("specInfo", ""),
                "产地": item.get("place", ""),
                "单位": item.get("unitInfo", ""),
                "发布日期": item.get("pubDate", ""),
            })
        
        df = pd.DataFrame(parsed)
        
        # 转换价格列为数值类型
        for col in ["最低价", "平均价", "最高价"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df
    
    def _generate_filename(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ext: str = "csv",
    ) -> str:
        """生成文件名"""
        if start_date and end_date:
            return f"xinfadi_price_{start_date}_to_{end_date}.{ext}"
        elif start_date:
            return f"xinfadi_price_{start_date}.{ext}"
        else:
            today = datetime.now().strftime("%Y-%m-%d")
            return f"xinfadi_price_{today}.{ext}"
    
    def save_to_csv(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """
        保存数据到CSV文件
        
        Args:
            df: 数据DataFrame
            filename: 文件名(可选)
            start_date: 开始日期(用于生成文件名)
            end_date: 结束日期(用于生成文件名)
            
        Returns:
            保存的文件路径
        """
        if filename is None:
            filename = self._generate_filename(start_date, end_date, "csv")
        
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        print(f"CSV已保存至: {filepath}")
        return filepath
    
    def save_to_excel(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """
        保存数据到Excel文件
        
        Args:
            df: 数据DataFrame
            filename: 文件名(可选)
            start_date: 开始日期(用于生成文件名)
            end_date: 结束日期(用于生成文件名)
            
        Returns:
            保存的文件路径
        """
        if filename is None:
            filename = self._generate_filename(start_date, end_date, "xlsx")
        elif not filename.endswith('.xlsx'):
            filename = filename.rsplit('.', 1)[0] + '.xlsx'
        
        filepath = os.path.join(self.output_dir, filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        print(f"Excel已保存至: {filepath}")
        return filepath
    
    def crawl_and_save(
        self,
        category: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        filename: Optional[str] = None,
            output_format: str = "csv",
            sync_to_feishu: bool = False,
    ) -> str:
        """
        一键抓取并保存数据
        
        Args:
            category: 分类筛选 (蔬菜/水果/肉禽蛋/水产/粮油/豆制品/调料)
            start_date: 开始日期
            end_date: 结束日期
            filename: 自定义文件名
            output_format: 输出格式 ("csv", "xlsx", "both")
            sync_to_feishu: 是否同步到飞书电子表格
            
        Returns:
            保存的文件路径
        """
        # 如果指定分类，先尝试API筛选；否则获取全部数据
        raw_data = self.fetch_all_data(
            category=category,
            start_date=start_date,
            end_date=end_date,
        )
        
        df = self.parse_data(raw_data)
        
        # 本地过滤确保分类准确（API筛选可能不精确）
        if category and not df.empty:
            original_count = len(df)
            df = df[df["一级分类"] == category]
            if len(df) < original_count:
                print(f"本地过滤: {original_count} -> {len(df)} 条 (分类: {category})")
        
        filepath = None
        
        # 保存本地文件
        if output_format in ("csv", "both"):
            filepath = self.save_to_csv(df, filename, start_date, end_date)
        
        if output_format in ("xlsx", "both"):
            xlsx_path = self.save_to_excel(df, filename, start_date, end_date)
            if filepath is None:
                filepath = xlsx_path
        
        # 同步到飞书
        if sync_to_feishu:
            self._sync_to_feishu(df, start_date, end_date)
        
        return filepath
    
    def _sync_to_feishu(
        self,
        df: pd.DataFrame,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """
        同步数据到飞书电子表格（每次创建新文件）
        
        Args:
            df: 数据DataFrame
            start_date: 开始日期
            end_date: 结束日期
        """
        if not FEISHU_AVAILABLE:
            print("警告: 飞书同步模块未安装，跳过同步")
            print("请确保 feishu_sync.py 文件存在")
            return
        
        print("\n正在同步到飞书...")
        
        try:
            client = load_sync_client()
            if client is None:
                print("飞书客户端初始化失败")
                print("请先运行: python feishu_sync.py --init")
                return
            
            # 生成电子表格文件标题
            if start_date and end_date:
                if start_date == end_date:
                    file_title = f"新发地价格_{start_date}"
                else:
                    file_title = f"新发地价格_{start_date}_to_{end_date}"
            else:
                today = datetime.now().strftime("%Y-%m-%d")
                file_title = f"新发地价格_{today}"
            
            # 上传到飞书（创建新文件，自动处理重名）
            result = client.upload_dataframe(df, file_title, auto_rename=True)
            
            if result:
                print("飞书同步成功!")
            else:
                print("飞书同步失败")
                
        except Exception as e:
            print(f"飞书同步异常: {e}")
    
    def crawl_today(self) -> str:
        """抓取今日数据"""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.crawl_and_save(start_date=today, end_date=today)
    
    def crawl_yesterday(self) -> str:
        """抓取昨日数据"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return self.crawl_and_save(start_date=yesterday, end_date=yesterday)
    
    def crawl_last_n_days(self, days: int = 7) -> str:
        """抓取最近N天数据"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days-1)).strftime("%Y-%m-%d")
        return self.crawl_and_save(start_date=start_date, end_date=end_date)


def run_scheduled_task(sync_to_feishu: bool = False, output_format: str = "csv"):
    """
    执行定时任务
    
    Args:
        sync_to_feishu: 是否同步到飞书
        output_format: 输出格式
    """
    print(f"\n{'='*50}")
    print(f"定时任务开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    
    crawler = XinfadiCrawler()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        crawler.crawl_and_save(
            start_date=today,
            end_date=today,
            output_format=output_format,
            sync_to_feishu=sync_to_feishu,
        )
    except Exception as e:
        print(f"抓取失败: {e}")


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description="新发地农产品价格数据爬虫",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 抓取今日数据
  python xinfadi_crawler.py --today
  
  # 抓取昨日数据
  python xinfadi_crawler.py --yesterday
  
  # 抓取最近7天数据
  python xinfadi_crawler.py --days 7
  
  # 抓取指定日期范围数据
  python xinfadi_crawler.py --start 2024-01-01 --end 2024-01-31
  
  # 抓取指定分类数据
  python xinfadi_crawler.py --category 蔬菜 --start 2024-01-01
  
  # 导出为Excel格式
  python xinfadi_crawler.py --today --format xlsx
  
  # 同时导出CSV和Excel
  python xinfadi_crawler.py --today --format both
  
  # 同步到飞书电子表格
  python xinfadi_crawler.py --today --sync-feishu
  
  # 启动定时任务(每天8:00自动抓取并同步)
  python xinfadi_crawler.py --schedule --sync-feishu
        """
    )
    
    parser.add_argument("--today", action="store_true", help="抓取今日数据")
    parser.add_argument("--yesterday", action="store_true", help="抓取昨日数据")
    parser.add_argument("--days", type=int, help="抓取最近N天数据")
    parser.add_argument("--start", type=str, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument(
        "--category", 
        type=str, 
        choices=list(XinfadiCrawler.CATEGORY_MAP.keys()),
        help="按分类筛选"
    )
    parser.add_argument("--output", type=str, default="./data", help="输出目录")
    parser.add_argument("--filename", type=str, help="自定义输出文件名")
    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "xlsx", "both"],
        default="csv",
        help="输出格式: csv, xlsx, both (默认: csv)"
    )
    parser.add_argument(
        "--sync-feishu",
        action="store_true",
        help="同步到飞书电子表格"
    )
    parser.add_argument("--schedule", action="store_true", help="启动定时任务模式")
    parser.add_argument(
        "--schedule-time", 
        type=str, 
        default="08:00",
        help="定时任务执行时间 (HH:MM), 默认 08:00"
    )
    
    args = parser.parse_args()
    
    crawler = XinfadiCrawler(output_dir=args.output)
    
    if args.schedule:
        # 定时任务模式
        import schedule
        from functools import partial
        
        print(f"定时任务已启动，将在每天 {args.schedule_time} 执行抓取")
        if args.sync_feishu:
            print("已开启飞书同步")
        print("按 Ctrl+C 停止...")
        
        # 创建带参数的任务函数
        task_func = partial(
            run_scheduled_task,
            sync_to_feishu=args.sync_feishu,
            output_format=args.format,
        )
        
        schedule.every().day.at(args.schedule_time).do(task_func)
        
        # 也可以立即执行一次
        task_func()
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    elif args.today:
        today = datetime.now().strftime("%Y-%m-%d")
        crawler.crawl_and_save(
            category=args.category,
            start_date=today,
            end_date=today,
            filename=args.filename,
            output_format=args.format,
            sync_to_feishu=args.sync_feishu,
        )
    
    elif args.yesterday:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        crawler.crawl_and_save(
            category=args.category,
            start_date=yesterday,
            end_date=yesterday,
            filename=args.filename,
            output_format=args.format,
            sync_to_feishu=args.sync_feishu,
        )
    
    elif args.days:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=args.days-1)).strftime("%Y-%m-%d")
        crawler.crawl_and_save(
            category=args.category,
            start_date=start_date,
            end_date=end_date,
            filename=args.filename,
            output_format=args.format,
            sync_to_feishu=args.sync_feishu,
        )
    
    elif args.start or args.end:
        crawler.crawl_and_save(
            category=args.category,
            start_date=args.start,
            end_date=args.end,
            filename=args.filename,
            output_format=args.format,
            sync_to_feishu=args.sync_feishu,
        )
    
    else:
        # 默认抓取今日数据
        print("未指定参数，默认抓取今日数据...")
        today = datetime.now().strftime("%Y-%m-%d")
        crawler.crawl_and_save(
            start_date=today,
            end_date=today,
            output_format=args.format,
            sync_to_feishu=args.sync_feishu,
        )


if __name__ == "__main__":
    main()


