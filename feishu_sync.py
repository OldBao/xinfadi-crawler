#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书电子表格同步模块
支持将数据上传到飞书电子表格（用户授权模式，保存到个人云盘）

使用前需要：
1. 访问 https://open.feishu.cn/ 创建应用
2. 获取 App ID 和 App Secret
3. 配置重定向URL: http://localhost:9000/callback
4. 添加以下权限（用户权限）：
   - sheets:spreadsheet (创建和编辑电子表格)
   - drive:drive (访问云空间)
5. 发布应用
6. 运行 python feishu_sync.py --auth 进行用户授权
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any


class FeishuSync:
    """飞书电子表格同步器（用户授权模式）"""
    
    # API 基础地址
    BASE_URL = "https://open.feishu.cn/open-apis"
    REDIRECT_URI = "http://localhost:9000/callback"
    
    def __init__(
        self,
        app_id: str,
        app_secret: str,
        folder_token: Optional[str] = None,
        config_file: str = "./feishu_config.json",
    ):
        """
        初始化飞书同步器
        
        Args:
            app_id: 飞书应用的 App ID
            app_secret: 飞书应用的 App Secret
            folder_token: 目标文件夹token（可选，默认保存到用户云盘根目录）
            config_file: 配置文件路径
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.folder_token = folder_token
        self.config_file = config_file
        
        # Token（支持两种模式：用户授权 或 应用授权）
        self.user_access_token = None
        self.refresh_token = None
        self.tenant_access_token = None
        self.token_expires_at = 0
        self.use_tenant_token = False  # 是否使用应用授权模式
        
        self.session = requests.Session()
        
        # 尝试从配置文件加载
        self._load_config()
    
    def _load_config(self):
        """从配置文件加载配置"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if not self.folder_token:
                        self.folder_token = config.get('folder_token', '')
                    self.user_access_token = config.get('user_access_token')
                    self.refresh_token = config.get('refresh_token')
                    self.token_expires_at = config.get('token_expires_at', 0)
                    self.use_tenant_token = config.get('use_tenant_token', False)
            except Exception as e:
                print(f"加载配置文件失败: {e}")
    
    def _save_config(self):
        """保存配置到文件"""
        try:
            # 先读取现有配置
            config = {}
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            
            # 更新 token 信息
            config['user_access_token'] = self.user_access_token
            config['refresh_token'] = self.refresh_token
            config['token_expires_at'] = self.token_expires_at
            config['updated_at'] = datetime.now().isoformat()
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"保存配置文件失败: {e}")
    
    def get_auth_url(self) -> str:
        """
        获取用户授权URL
        参考文档: https://open.feishu.cn/document/authentication-management/access-token/obtain-oauth-code
        
        Returns:
            授权URL
        """
        import urllib.parse
        
        # 飞书OAuth授权URL（根据官方文档）
        params = {
            "app_id": self.app_id,
            "redirect_uri": self.REDIRECT_URI,
            "state": "feishu_sync",
        }
        query = urllib.parse.urlencode(params)
        # 使用 /authen/v1/index 端点（官方推荐的用户登录授权页面）
        return f"https://open.feishu.cn/open-apis/authen/v1/index?{query}"
    
    def exchange_code_for_token(self, code: str) -> bool:
        """
        使用授权码换取 user_access_token
        
        Args:
            code: 授权码
            
        Returns:
            是否成功
        """
        # 先获取 app_access_token
        app_token_url = f"{self.BASE_URL}/auth/v3/app_access_token/internal"
        app_token_data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        }
        
        try:
            response = self.session.post(app_token_url, json=app_token_data)
            result = response.json()
            
            if result.get('code') != 0:
                print(f"获取app_access_token失败: {result.get('msg')}")
                return False
            
            app_access_token = result.get('app_access_token')
            
            # 使用 code 换取 user_access_token
            user_token_url = f"{self.BASE_URL}/authen/v1/oidc/access_token"
            headers = {
                "Authorization": f"Bearer {app_access_token}",
                "Content-Type": "application/json",
            }
            user_token_data = {
                "grant_type": "authorization_code",
                "code": code,
            }
            
            response = self.session.post(user_token_url, headers=headers, json=user_token_data)
            result = response.json()
            
            if result.get('code') == 0:
                data = result.get('data', {})
                self.user_access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')
                expire = data.get('expires_in', 7200)
                self.token_expires_at = datetime.now().timestamp() + expire
                self._save_config()
                print("✓ 用户授权成功!")
                print(f"  用户: {data.get('name', 'Unknown')}")
                return True
            else:
                print(f"获取user_access_token失败: {result.get('msg')}")
                return False
                
        except Exception as e:
            print(f"授权异常: {e}")
            return False
    
    def refresh_user_token(self) -> bool:
        """刷新 user_access_token"""
        if not self.refresh_token:
            print("没有refresh_token，请重新授权")
            return False
        
        # 先获取 app_access_token
        app_token_url = f"{self.BASE_URL}/auth/v3/app_access_token/internal"
        app_token_data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        }
        
        try:
            response = self.session.post(app_token_url, json=app_token_data)
            result = response.json()
            
            if result.get('code') != 0:
                return False
            
            app_access_token = result.get('app_access_token')
            
            # 刷新 user_access_token
            refresh_url = f"{self.BASE_URL}/authen/v1/oidc/refresh_access_token"
            headers = {
                "Authorization": f"Bearer {app_access_token}",
                "Content-Type": "application/json",
            }
            refresh_data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }
            
            response = self.session.post(refresh_url, headers=headers, json=refresh_data)
            result = response.json()
            
            if result.get('code') == 0:
                data = result.get('data', {})
                self.user_access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')
                expire = data.get('expires_in', 7200)
                self.token_expires_at = datetime.now().timestamp() + expire
                self._save_config()
                return True
            else:
                print(f"刷新token失败: {result.get('msg')}")
                return False
                
        except Exception as e:
            print(f"刷新token异常: {e}")
            return False
    
    def get_user_access_token(self) -> Optional[str]:
        """
        获取有效的 user_access_token（自动刷新）
        
        Returns:
            user_access_token
        """
        if not self.user_access_token:
            print("未授权，请先运行: python feishu_sync.py --auth")
            return None
        
        # 检查 token 是否有效
        current_time = datetime.now().timestamp()
        if current_time >= self.token_expires_at - 300:
            # token 即将过期，尝试刷新
            if not self.refresh_user_token():
                print("Token已过期，请重新授权: python feishu_sync.py --auth")
                return None
        
        return self.user_access_token
    
    def get_tenant_access_token(self) -> Optional[str]:
        """
        获取应用授权token (tenant_access_token)
        适合个人使用，不需要浏览器授权
        """
        current_time = datetime.now().timestamp()
        
        # 检查缓存的token是否有效
        if self.tenant_access_token and current_time < self.token_expires_at - 300:
            return self.tenant_access_token
        
        url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        }
        
        try:
            response = self.session.post(url, json=data)
            result = response.json()
            
            if result.get('code') == 0:
                self.tenant_access_token = result.get('tenant_access_token')
                expire = result.get('expire', 7200)
                self.token_expires_at = current_time + expire
                return self.tenant_access_token
            else:
                print(f"获取tenant_access_token失败: {result.get('msg')}")
                return None
        except Exception as e:
            print(f"获取token异常: {e}")
            return None
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头（自动选择token类型）"""
        if self.use_tenant_token:
            token = self.get_tenant_access_token()
        else:
            token = self.get_user_access_token()
        
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    
    def enable_simple_mode(self):
        """
        启用简单模式（应用授权），不需要浏览器授权
        适合个人使用
        """
        self.use_tenant_token = True
        
        # 测试token是否有效
        token = self.get_tenant_access_token()
        if token:
            # 保存配置
            try:
                config = {}
                if os.path.exists(self.config_file):
                    with open(self.config_file, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                
                config['use_tenant_token'] = True
                config['updated_at'] = datetime.now().isoformat()
                
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                
                print("✓ 简单模式已启用！")
                print("  无需浏览器授权，直接使用应用凭证")
                return True
            except Exception as e:
                print(f"保存配置失败: {e}")
                return False
        else:
            print("✗ 应用凭证无效，请检查 app_id 和 app_secret")
            return False
    
    def list_folder_files(self, folder_token: Optional[str] = None) -> List[Dict]:
        """
        列出文件夹中的文件
        
        Args:
            folder_token: 文件夹token，空表示根目录
            
        Returns:
            文件列表
        """
        url = f"{self.BASE_URL}/drive/v1/files"
        params = {
            "page_size": 100,
        }
        if folder_token:
            params["folder_token"] = folder_token
        
        try:
            response = self.session.get(url, headers=self._get_headers(), params=params)
            result = response.json()
            
            if result.get('code') == 0:
                return result.get('data', {}).get('files', [])
            else:
                # 可能没有权限，返回空列表
                return []
        except Exception as e:
            print(f"列出文件异常: {e}")
            return []
    
    def get_unique_filename(self, base_name: str, folder_token: Optional[str] = None) -> str:
        """
        获取唯一的文件名，如果存在则添加 _(1), _(2) 等后缀
        
        Args:
            base_name: 基础文件名
            folder_token: 文件夹token
            
        Returns:
            唯一的文件名
        """
        files = self.list_folder_files(folder_token or self.folder_token)
        existing_names = {f.get('name', '') for f in files}
        
        if base_name not in existing_names:
            return base_name
        
        counter = 1
        while True:
            new_name = f"{base_name}_({counter})"
            if new_name not in existing_names:
                return new_name
            counter += 1
    
    def create_spreadsheet(
        self,
        title: str,
        folder_token: Optional[str] = None,
    ) -> Optional[str]:
        """
        创建新的电子表格文件
        
        Args:
            title: 电子表格标题
            folder_token: 目标文件夹token（可选）
            
        Returns:
            新电子表格的 spreadsheet_token
        """
        url = f"{self.BASE_URL}/sheets/v3/spreadsheets"
        
        data = {
            "title": title,
        }
        
        # 如果指定了文件夹，需要用不同的API
        target_folder = folder_token or self.folder_token
        if target_folder:
            data["folder_token"] = target_folder
        
        try:
            response = self.session.post(url, headers=self._get_headers(), json=data)
            result = response.json()
            
            if result.get('code') == 0:
                spreadsheet = result.get('data', {}).get('spreadsheet', {})
                token = spreadsheet.get('spreadsheet_token')
                sheet_url = spreadsheet.get('url', '')
                print(f"电子表格创建成功: {title}")
                if sheet_url:
                    print(f"访问链接: {sheet_url}")
                return token
            else:
                print(f"创建电子表格失败: {result.get('msg')}")
                print(f"错误码: {result.get('code')}")
                return None
        except Exception as e:
            print(f"创建电子表格异常: {e}")
            return None
    
    def get_spreadsheet_info(self, spreadsheet_token: str) -> Optional[Dict]:
        """
        获取电子表格信息
        
        Args:
            spreadsheet_token: 电子表格token
            
        Returns:
            表格信息
        """
        token = spreadsheet_token or self.spreadsheet_token
        if not token:
            print("未指定 spreadsheet_token")
            return None
        
        url = f"{self.BASE_URL}/sheets/v3/spreadsheets/{token}"
        
        try:
            response = self.session.get(url, headers=self._get_headers())
            result = response.json()
            
            if result.get('code') == 0:
                return result.get('data', {}).get('spreadsheet', {})
            else:
                print(f"获取表格信息失败: {result.get('msg')}")
                return None
        except Exception as e:
            print(f"获取表格信息异常: {e}")
            return None
    
    def get_sheets(self, spreadsheet_token: Optional[str] = None) -> List[Dict]:
        """
        获取电子表格中的所有工作表
        
        Args:
            spreadsheet_token: 电子表格token
            
        Returns:
            工作表列表
        """
        token = spreadsheet_token or self.spreadsheet_token
        if not token:
            print("未指定 spreadsheet_token")
            return []
        
        url = f"{self.BASE_URL}/sheets/v3/spreadsheets/{token}/sheets/query"
        
        try:
            response = self.session.get(url, headers=self._get_headers())
            result = response.json()
            
            if result.get('code') == 0:
                return result.get('data', {}).get('sheets', [])
            else:
                print(f"获取工作表失败: {result.get('msg')}")
                return []
        except Exception as e:
            print(f"获取工作表异常: {e}")
            return []
    
    def create_sheet(
        self,
        title: str,
        spreadsheet_token: Optional[str] = None,
    ) -> Optional[str]:
        """
        在电子表格中创建新工作表
        
        Args:
            title: 工作表标题
            spreadsheet_token: 电子表格token
            
        Returns:
            新工作表的 sheet_id
        """
        token = spreadsheet_token or self.spreadsheet_token
        if not token:
            print("未指定 spreadsheet_token")
            return None
        
        url = f"{self.BASE_URL}/sheets/v2/spreadsheets/{token}/sheets_batch_update"
        
        data = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": title,
                            "index": 0,
                        }
                    }
                }
            ]
        }
        
        try:
            response = self.session.post(url, headers=self._get_headers(), json=data)
            result = response.json()
            
            if result.get('code') == 0:
                replies = result.get('data', {}).get('replies', [])
                if replies:
                    sheet_id = replies[0].get('addSheet', {}).get('properties', {}).get('sheetId')
                    print(f"工作表创建成功: {title}")
                    return sheet_id
            else:
                print(f"创建工作表失败: {result.get('msg')}")
                return None
        except Exception as e:
            print(f"创建工作表异常: {e}")
            return None
    
    def get_unique_sheet_title(
        self,
        base_title: str,
        spreadsheet_token: Optional[str] = None,
    ) -> str:
        """
        获取唯一的工作表标题，如果存在则添加 _(1), _(2) 等后缀
        
        Args:
            base_title: 基础标题
            spreadsheet_token: 电子表格token
            
        Returns:
            唯一的标题
        """
        sheets = self.get_sheets(spreadsheet_token)
        existing_titles = {s.get('title', '') for s in sheets}
        
        if base_title not in existing_titles:
            return base_title
        
        counter = 1
        while True:
            new_title = f"{base_title}_({counter})"
            if new_title not in existing_titles:
                return new_title
            counter += 1
    
    def write_data(
        self,
        data: List[List[Any]],
        sheet_id: str,
        start_cell: str = "A1",
        spreadsheet_token: Optional[str] = None,
        batch_size: int = 4000,  # 飞书单次写入限制约5000行，保守使用4000
    ) -> bool:
        """
        写入数据到工作表（支持分批写入大数据集）
        
        Args:
            data: 二维数组数据
            sheet_id: 工作表ID
            start_cell: 起始单元格
            spreadsheet_token: 电子表格token
            batch_size: 每批写入的行数
            
        Returns:
            是否成功
        """
        token = spreadsheet_token or self.spreadsheet_token
        if not token:
            print("未指定 spreadsheet_token")
            return False
        
        if not data:
            return True
            
        cols = max(len(row) for row in data) if data else 0
        
        # 将列数转换为字母
        def col_to_letter(col):
            result = ""
            while col > 0:
                col -= 1
                result = chr(col % 26 + ord('A')) + result
                col //= 26
            return result
        
        end_col = col_to_letter(cols)
        
        # 解析起始单元格
        import re
        match = re.match(r'^([A-Z]+)(\d+)$', start_cell.upper())
        if match:
            start_row = int(match.group(2))
        else:
            start_row = 1
        
        url = f"{self.BASE_URL}/sheets/v2/spreadsheets/{token}/values"
        
        # 分批写入
        total_rows = len(data)
        batches = (total_rows + batch_size - 1) // batch_size
        
        for batch_idx in range(batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, total_rows)
            batch_data = data[batch_start:batch_end]
            
            current_start_row = start_row + batch_start
            current_end_row = start_row + batch_end - 1
            range_str = f"{sheet_id}!A{current_start_row}:{end_col}{current_end_row}"
            
            request_data = {
                "valueRange": {
                    "range": range_str,
                    "values": batch_data,
                }
            }
            
            try:
                response = self.session.put(url, headers=self._get_headers(), json=request_data)
                result = response.json()
                
                if result.get('code') != 0:
                    print(f"写入数据失败 (批次 {batch_idx + 1}/{batches}): {result.get('msg')}")
                    return False
                    
                if batches > 1:
                    print(f"写入进度: {batch_end}/{total_rows} 行 ({batch_idx + 1}/{batches})")
                    
            except Exception as e:
                print(f"写入数据异常: {e}")
                return False
        
        return True
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        file_title: str,
        folder_token: Optional[str] = None,
        auto_rename: bool = True,
    ) -> Optional[str]:
        """
        将DataFrame上传到飞书，创建新的电子表格文件
        
        Args:
            df: pandas DataFrame
            file_title: 电子表格文件标题
            folder_token: 目标文件夹token
            auto_rename: 是否自动重命名避免冲突
            
        Returns:
            新电子表格的token，失败返回None
        """
        target_folder = folder_token or self.folder_token
        
        # 处理文件名冲突
        if auto_rename:
            file_title = self.get_unique_filename(file_title, target_folder)
        
        # 创建新的电子表格文件
        spreadsheet_token = self.create_spreadsheet(file_title, target_folder)
        if not spreadsheet_token:
            return None
        
        # 获取默认工作表ID
        sheets = self.get_sheets(spreadsheet_token)
        if not sheets:
            print("无法获取工作表")
            return None
        
        sheet_id = sheets[0].get('sheet_id')
        
        # 准备数据（表头 + 数据）
        headers = df.columns.tolist()
        values = df.values.tolist()
        
        # 转换数据类型为可JSON序列化的格式
        def convert_value(v):
            if pd.isna(v):
                return ""
            if isinstance(v, (int, float)):
                return v
            return str(v)
        
        data = [headers]
        for row in values:
            data.append([convert_value(v) for v in row])
        
        # 写入数据
        success = self.write_data(data, sheet_id, "A1", spreadsheet_token)
        
        if success:
            print(f"数据已上传到飞书: {file_title} ({len(df)} 行)")
            return spreadsheet_token
        
        return None


def create_config_template():
    """创建配置文件模板"""
    template = {
        "app_id": "YOUR_APP_ID",
        "app_secret": "YOUR_APP_SECRET",
        "folder_token": "",
        "user_access_token": "",
        "refresh_token": "",
        "token_expires_at": 0,
        "instructions": {
            "step1": "访问 https://open.feishu.cn/ 创建应用",
            "step2": "在【安全设置】中添加重定向URL: http://localhost:9000/callback",
            "step3": "在【权限管理】中添加以下权限:",
            "permissions": [
                "sheets:spreadsheet - 创建和编辑电子表格",
                "drive:drive - 访问云空间",
            ],
            "step4": "发布应用版本（创建版本并发布）",
            "step5": "将 app_id、app_secret 填入此文件",
            "step6": "运行 python feishu_sync.py --auth 进行用户授权",
            "note_folder": "folder_token 可选，如果要保存到特定文件夹，从文件夹URL获取",
            "note_folder_url": "格式如: https://xxx.feishu.cn/drive/folder/TOKEN"
        }
    }
    
    config_path = "./feishu_config.json"
    if not os.path.exists(config_path):
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(template, f, indent=2, ensure_ascii=False)
        print(f"配置文件模板已创建: {config_path}")
        print("\n请按以下步骤配置:")
        print("1. 访问 https://open.feishu.cn/ 创建应用")
        print("2. 在【安全设置】添加重定向URL: http://localhost:9000/callback")
        print("3. 添加【sheets:spreadsheet】和【drive:drive】权限")
        print("4. 发布应用版本")
        print("5. 编辑配置文件填入 app_id, app_secret")
        print("6. 运行 python feishu_sync.py --auth 进行用户授权")
    else:
        print(f"配置文件已存在: {config_path}")
    
    return config_path


def load_sync_client(config_file: str = "./feishu_config.json") -> Optional[FeishuSync]:
    """
    从配置文件加载同步客户端
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        FeishuSync 实例
    """
    if not os.path.exists(config_file):
        print(f"配置文件不存在: {config_file}")
        print("请先运行: python feishu_sync.py --init")
        return None
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        app_id = config.get('app_id', '')
        app_secret = config.get('app_secret', '')
        folder_token = config.get('folder_token', '')
        
        if not app_id or app_id == "YOUR_APP_ID":
            print("请先配置 app_id")
            return None
        
        if not app_secret or app_secret == "YOUR_APP_SECRET":
            print("请先配置 app_secret")
            return None
        
        return FeishuSync(
            app_id=app_id,
            app_secret=app_secret,
            folder_token=folder_token,
            config_file=config_file,
        )
    except Exception as e:
        print(f"加载配置失败: {e}")
        return None


def start_auth_server(client: FeishuSync):
    """启动本地服务器接收OAuth回调"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import urllib.parse
    import webbrowser
    import socket
    
    auth_code = [None]  # 使用列表来存储，避免闭包问题
    server_done = [False]
    
    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            # 解析URL参数
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            
            if 'code' in params:
                auth_code[0] = params['code'][0]
                server_done[0] = True
                
                # 返回成功页面
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                
                html = """
                <html>
                <head><title>授权成功</title></head>
                <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
                    <h1>✅ 授权成功！</h1>
                    <p>您可以关闭此页面，返回终端查看结果。</p>
                </body>
                </html>
                """
                self.wfile.write(html.encode('utf-8'))
            else:
                server_done[0] = True
                self.send_response(400)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                error_msg = params.get('error', ['未知错误'])[0]
                self.wfile.write(f'<h1>授权失败: {error_msg}</h1>'.encode('utf-8'))
        
        def log_message(self, format, *args):
            pass  # 禁止日志输出
    
    # 检查端口是否被占用
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.bind(('localhost', 9000))
        test_socket.close()
    except OSError:
        print("错误: 端口 9000 被占用，请关闭其他程序后重试")
        return False
    
    # 启动服务器
    try:
        server = HTTPServer(('localhost', 9000), CallbackHandler)
        server.timeout = 5  # 每5秒检查一次
    except Exception as e:
        print(f"启动服务器失败: {e}")
        return False
    
    # 打开浏览器
    auth_url = client.get_auth_url()
    print("\n" + "="*60)
    print("请在浏览器中完成授权")
    print("="*60)
    print(f"\n授权链接:\n{auth_url}\n")
    print("如果浏览器未自动打开，请手动复制上面的链接到浏览器中打开")
    print("\n等待授权回调... (按 Ctrl+C 取消)\n")
    
    try:
        webbrowser.open(auth_url)
    except:
        pass
    
    # 等待回调（最多等待5分钟）
    import time
    start_time = time.time()
    timeout = 300  # 5分钟超时
    
    try:
        while not server_done[0]:
            server.handle_request()
            if time.time() - start_time > timeout:
                print("\n授权超时（5分钟）")
                break
    except KeyboardInterrupt:
        print("\n用户取消授权")
        server.server_close()
        return False
    
    server.server_close()
    
    if auth_code[0]:
        print("\n收到授权码，正在换取Token...")
        return client.exchange_code_for_token(auth_code[0])
    else:
        print("\n未收到授权码")
        print("\n请检查:")
        print("1. 飞书开放平台是否添加了重定向URL: http://localhost:9000/callback")
        print("2. 应用是否已发布（创建版本 -> 发布）")
        print("3. 是否添加了 sheets:spreadsheet 和 drive:drive 权限")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="飞书电子表格同步工具")
    parser.add_argument("--init", action="store_true", help="创建配置文件模板")
    parser.add_argument("--simple", action="store_true", help="启用简单模式（推荐个人使用，无需浏览器授权）")
    parser.add_argument("--auth", action="store_true", help="进行用户授权（会打开浏览器）")
    parser.add_argument("--auth-manual", action="store_true", help="手动输入授权码（备用方案）")
    parser.add_argument("--test", action="store_true", help="测试连接")
    parser.add_argument("--list-files", action="store_true", help="列出云空间文件")
    parser.add_argument("--upload", type=str, help="上传CSV文件创建新电子表格")
    parser.add_argument("--title", type=str, help="电子表格标题")
    
    args = parser.parse_args()
    
    if args.init:
        create_config_template()
    
    elif args.simple:
        client = load_sync_client()
        if client:
            print("\n启用简单模式（应用授权）...")
            if client.enable_simple_mode():
                print("\n现在可以使用 --sync-feishu 同步数据了！")
                print("文件将创建在应用关联的云空间中")
    
    elif args.auth:
        client = load_sync_client()
        if client:
            success = start_auth_server(client)
            if success:
                print("\n授权完成! 现在可以使用 --sync-feishu 同步数据到您的云盘")
    
    elif args.auth_manual:
        client = load_sync_client()
        if client:
            auth_url = client.get_auth_url()
            print("\n" + "="*60)
            print("手动授权模式")
            print("="*60)
            print(f"\n1. 请在浏览器中打开:\n{auth_url}\n")
            print("2. 完成授权后，浏览器会跳转到一个无法访问的页面")
            print("3. 从浏览器地址栏复制完整的URL")
            print("   (格式如: http://localhost:9000/callback?code=xxxxx)")
            print()
            
            url = input("请粘贴完整URL: ").strip()
            
            if url:
                import urllib.parse
                parsed = urllib.parse.urlparse(url)
                params = urllib.parse.parse_qs(parsed.query)
                
                if 'code' in params:
                    code = params['code'][0]
                    if client.exchange_code_for_token(code):
                        print("\n授权完成! 现在可以使用 --sync-feishu 同步数据")
                else:
                    print("URL中没有找到授权码")
    
    elif args.test:
        client = load_sync_client()
        if client:
            print("\n测试飞书API连接...")
            
            if client.use_tenant_token:
                print("模式: 简单模式（应用授权）")
                token = client.get_tenant_access_token()
            else:
                print("模式: 用户授权")
                token = client.get_user_access_token()
            
            if token:
                print("✓ Token有效")
                
                # 尝试列出文件夹（测试云空间权限）
                files = client.list_folder_files()
                print(f"✓ 云空间文件数量: {len(files)}")
                
                print("\n连接测试通过!")
            else:
                print("✗ Token无效")
                print("  - 简单模式: 运行 python feishu_sync.py --simple")
                print("  - 用户授权: 运行 python feishu_sync.py --auth")
    
    elif args.list_files:
        client = load_sync_client()
        if client:
            files = client.list_folder_files()
            print(f"\n云空间文件列表 (共 {len(files)} 个):")
            for f in files:
                print(f"  - {f.get('name')} ({f.get('type')})")
    
    elif args.upload:
        client = load_sync_client()
        if client:
            if not os.path.exists(args.upload):
                print(f"文件不存在: {args.upload}")
            else:
                df = pd.read_csv(args.upload)
                title = args.title or os.path.basename(args.upload).rsplit('.', 1)[0]
                client.upload_dataframe(df, title)
    
    else:
        parser.print_help()

