#!/usr/bin/env python3
"""Setup database for GitHub Actions testing"""
import psycopg2
import sys

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="postgres123",
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("SELECT 1 FROM pg_database WHERE datname='ecommerce'")
if cursor.fetchone() is None:
    cursor.execute("CREATE DATABASE ecommerce")
    print("Created ecommerce database")

cursor.close()
conn.close()

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce",
    user="postgres",
    password="postgres123",
)
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(50) DEFAULT 'pending',
        total_amount DECIMAL(10, 2) NOT NULL,
        shipping_address TEXT
    )
"""
)
conn.commit()

cursor.execute(
    """
    INSERT INTO orders (customer_id, total_amount, status, shipping_address)
    VALUES
        (1, 99.99, 'pending', '123 Main St'),
        (2, 149.50, 'processing', '456 Oak Ave'),
        (3, 75.00, 'shipped', '789 Pine Rd')
    ON CONFLICT DO NOTHING
"""
)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM orders")
count = cursor.fetchone()[0]

cursor.close()
conn.close()

print(f"Database setup complete: {count} orders")
sys.exit(0)
