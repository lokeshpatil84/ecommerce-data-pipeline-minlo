#!/usr/bin/env python3
"""Run pipeline test"""
import psycopg2
import time
import random
import sys

print("Running E-Commerce Pipeline Test")
print("=" * 50)

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce",
    user="postgres",
    password="postgres123"
)
cursor = conn.cursor()

statuses = ["pending", "processing", "shipped", "delivered"]

for i in range(5):
    customer_id = random.randint(1, 100)
    total_amount = round(random.uniform(10, 1000), 2)
    status = random.choice(statuses)
    address = f"Test Address {random.randint(1, 1000)}"
    
    cursor.execute("""
        INSERT INTO orders (customer_id, total_amount, status, shipping_address)
        VALUES (%s, %s, %s, %s)
        RETURNING order_id
    """, (customer_id, total_amount, status, address))
    
    order_id = cursor.fetchone()[0]
    conn.commit()
    print(f"Created order #{order_id}: {total_amount} - {status}")
    time.sleep(0.5)

cursor.execute("SELECT COUNT(*) FROM orders")
total_count = cursor.fetchone()[0]

cursor.execute("SELECT SUM(total_amount) FROM orders")
total_revenue = cursor.fetchone()[0] or 0

print()
print("Pipeline Test Results:")
print(f"Total orders: {total_count}")
print(f"Total revenue: {total_revenue:,.2f}")

cursor.close()
conn.close()

print()
print("Pipeline test completed!")
sys.exit(0)

