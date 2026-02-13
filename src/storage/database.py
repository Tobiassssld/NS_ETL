# src/storage/database.py

import sqlite3
from pathlib import Path

class Database:
    """
    数据库管理器
    """
    
    def __init__(self, db_path="data/nl_rail.db"):
        """
        初始化数据库连接
        """
        self.db_path = db_path
        
        # 确保data文件夹存在
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 连接数据库（如果不存在会自动创建）
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        print(f"📁 数据库位置: {db_path}")
    
    def initialize_schema(self):
        """
        执行schema.sql文件，创建所有表
        """
        schema_path = Path("src/storage/schema.sql")
        
        # 读取SQL文件
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # 执行（SQLite允许一次执行多条语句）
        self.cursor.executescript(schema_sql)
        self.conn.commit()
        
        print("✅ 数据库表结构创建成功！")
    
    def show_tables(self):
        """
        显示数据库里有哪些表
        """
        self.cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name;
        """)
        
        tables = self.cursor.fetchall()
        
        print("\n📋 数据库中的表：")
        for table in tables:
            print(f"   - {table[0]}")
    
    def close(self):
        """
        关闭连接
        """
        self.conn.close()
        print("🔒 数据库连接已关闭")


# ===== 测试代码 =====
if __name__ == "__main__":
    print("=== 数据库初始化测试 ===\n")
    
    db = Database()
    db.initialize_schema()
    db.show_tables()
    
    # 测试：查询车站数据
    print("\n🚉 预装的车站数据：")
    db.cursor.execute("SELECT station_code, station_name FROM stations;")
    for row in db.cursor.fetchall():
        print(f"   {row[0]} - {row[1]}")
    
    db.close()