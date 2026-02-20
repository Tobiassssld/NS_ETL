# src/storage/database.py

import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

AZURE_SQL_CONN_STR = os.getenv('AZURE_SQL_CONNECTION_STRING')

class Database:
    """
    数据库管理器
    自动切换：有 AZURE_SQL_CONNECTION_STRING 就连Azure，否则用本地SQLite
    """

    def __init__(self, db_path="data/nl_rail.db"):

        if AZURE_SQL_CONN_STR:
            # Azure SQL 模式
            import pyodbc
            self.mode = 'azure'
            self.conn = pyodbc.connect(AZURE_SQL_CONN_STR)
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            print("☁️  Connected to Azure SQL Database")

        else:
            # 本地 SQLite 模式
            self.mode = 'sqlite'
            self.db_path = db_path
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            print(f"📁 Connected to SQLite: {db_path}")

    def initialize_schema(self):
        """
        仅在SQLite模式下使用（Azure SQL的表已通过schema_azure.sql手动创建）
        """
        if self.mode == 'azure':
            print("☁️  Azure SQL: 跳过schema初始化（表已存在）")
            return

        schema_path = Path("src/storage/schema.sql")
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        self.cursor.executescript(schema_sql)
        self.conn.commit()
        print("✅ SQLite schema初始化成功")

    def show_tables(self):
        """
        显示数据库里有哪些表
        """
        if self.mode == 'azure':
            self.cursor.execute("""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
        else:
            self.cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name
            """)

        tables = self.cursor.fetchall()
        print("\n📋 数据库中的表：")
        for table in tables:
            print(f"   - {table[0]}")

    def close(self):
        self.conn.close()
        print("🔒 数据库连接已关闭")