import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv()
engine = create_engine(os.getenv("sql_database"))
inspector = inspect(engine)

tables = inspector.get_table_names()
# print("Tables in DB:", tables)
for table_name in tables:
    print(f"\n📘 Table: {table_name}")
    columns = inspector.get_columns(table_name)
    for col in columns:
        print(f"  - {col['name']} ({col['type']})")

assert "game_analyses" in tables
assert "player_analyses" in tables

print("OK")
