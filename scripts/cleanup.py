#!/usr/bin/env python3
"""Cleanup test data"""
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce",
    user="postgres",
    password="postgres123"
)
cursor = conn.cursor()
cursor.execute("DELETE FROM orders WHERE order_id > 3")
deleted = cursor.rowcount
conn.commit()
cursor.close()
conn.close()
print(f"Cleaned up {deleted} test records")