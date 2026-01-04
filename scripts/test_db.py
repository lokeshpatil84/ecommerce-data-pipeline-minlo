#!/usr/bin/env python3
"""Test database connection"""
import sys

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce",
    user="postgres",
    password="postgres123",
)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM orders")
count = cursor.fetchone()[0]
cursor.close()
conn.close()
print(f"Database connection successful: {count} orders")
sys.exit(0)
