import psycopg2
import time
import random
from datetime import datetime

def insert_test_orders(num_orders=10):
    """Insert test orders into PostgreSQL"""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce",
        user="postgres",
        password="postgres123"
    )
    
    cursor = conn.cursor()
    
    statuses = ['pending', 'processing', 'shipped', 'delivered']
    
    for i in range(num_orders):
        customer_id = random.randint(1, 3)
        total_amount = round(random.uniform(10, 500), 2)
        status = random.choice(statuses)
        
        cursor.execute("""
            INSERT INTO ecommerce.orders (customer_id, total_amount, status, shipping_address)
            VALUES (%s, %s, %s, %s)
            RETURNING order_id
        """, (customer_id, total_amount, status, f"Test Address {i}"))
        
        order_id = cursor.fetchone()[0]
        print(f"Created order {order_id}: ${total_amount} - {status}")
        
        conn.commit()
        time.sleep(1)  # Simulate real-time data
    
    cursor.close()
    conn.close()
    print(f"\n✓ Successfully created {num_orders} test orders")

if __name__ == "__main__":
    print("🧪 Testing E-Commerce Data Pipeline\n")
    insert_test_orders(10)
    
    print("\nPipeline test complete!")
    print("Check the following:")
    print("  1. Kafka UI: http://localhost:8080")
    print("  2. Monitor Glue jobs in AWS Console")
    print("  3. Query Iceberg tables via Athena")