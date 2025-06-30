import duckdb
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

# Initialize Faker
fake = Faker()

# Nepal-specific data
NEPAL_FIRST_NAMES = [
    'Ramesh', 'Sita', 'Arjun', 'Kamala', 'Bikash', 'Sunita', 'Rajesh', 'Gita',
    'Suresh', 'Maya', 'Prakash', 'Laxmi', 'Santosh', 'Radha', 'Deepak', 'Sushma',
    'Mahesh', 'Puja', 'Naresh', 'Bimala', 'Ravi', 'Shanti', 'Kamal', 'Devi',
    'Roshan', 'Sapana', 'Bijay', 'Manju', 'Rajan', 'Sarita', 'Dinesh', 'Prabha',
    'Anil', 'Nirmala', 'Manoj', 'Kamana', 'Binod', 'Rita', 'Ashok', 'Geeta'
]

NEPAL_LAST_NAMES = [
    'Shrestha', 'Sharma', 'Maharjan', 'Pradhan', 'Tamang', 'Gurung', 'Rai',
    'Limbu', 'Thapa', 'Magar', 'Karki', 'Adhikari', 'Khadka', 'Poudel',
    'Bhattarai', 'Acharya', 'Koirala', 'Dahal', 'Ghimire', 'Regmi',
    'Dhakal', 'Bastola', 'Chapagain', 'Panta', 'Bhandari', 'Ojha', 'Joshi',
    'Pandey', 'Mishra', 'Upreti', 'Devkota', 'Sapkota', 'Kafle', 'Tiwari'
]

PAYMENT_METHODS = ['eSewa', 'Khalti', 'IME Pay', 'Cash on Delivery', 'Bank Transfer', 'Credit Card', 'Debit Card']
PAYMENT_WEIGHTS = [0.25, 0.2, 0.15, 0.25, 0.05, 0.05, 0.05]  # Higher weight for eSewa and COD

ORDER_STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']
ORDER_STATUS_WEIGHTS = [0.05, 0.1, 0.15, 0.6, 0.08, 0.02]

PAYMENT_STATUSES = ['pending', 'completed', 'failed', 'refunded']
PAYMENT_STATUS_WEIGHTS = [0.05, 0.85, 0.08, 0.02]

PRODUCT_CATEGORIES = {
    'Electronics': ['Mobile Phone', 'Laptop', 'Headphones', 'Speaker', 'Charger', 'Power Bank', 'Camera'],
    'Fashion': ['T-Shirt', 'Jeans', 'Kurta', 'Saree', 'Shoes', 'Sandals', 'Bag', 'Watch'],
    'Home & Kitchen': ['Rice Cooker', 'Blender', 'Pressure Cooker', 'Thermos', 'Dinner Set', 'Curtain'],
    'Books': ['Novel', 'Textbook', 'Dictionary', 'Magazine', 'Comic Book'],
    'Health & Beauty': ['Face Cream', 'Shampoo', 'Soap', 'Toothpaste', 'Perfume', 'Makeup Kit'],
    'Sports': ['Football', 'Cricket Bat', 'Badminton Racket', 'Running Shoes', 'Yoga Mat'],
    'Groceries': ['Rice', 'Dal', 'Oil', 'Spices', 'Tea', 'Biscuits', 'Noodles']
}

BRANDS = [
    'Samsung', 'Apple', 'Xiaomi', 'Dell', 'HP', 'Sony', 'LG', 'Panasonic',
    'Nike', 'Adidas', 'Puma', 'Zara', 'H&M', 'Local Brand', 'Generic',
    'Dabur', 'Himalaya', 'Unilever', 'P&G', 'Goldstar', 'CG', 'Bajaj'
]

def generate_customers(n=12000):
    """Generate customer data"""
    customers = []
    
    for i in range(1, n + 1):
        first_name = random.choice(NEPAL_FIRST_NAMES)
        last_name = random.choice(NEPAL_LAST_NAMES)
        
        # Generate realistic email
        email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'email.com'])
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{email_domain}"
        
        # Nepal phone format
        phone = f"+977-{random.randint(980, 989)}-{random.randint(1000000, 9999999)}"
        
        # Generate realistic birth date (18-70 years old)
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=70)
        
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        
        customers.append({
            'customer_id': i,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'date_of_birth': birth_date,
            'gender': random.choice(['Male', 'Female']),
            'created_at': created_at,
            'updated_at': created_at + timedelta(days=random.randint(0, 30))
        })
    
    return customers

def generate_products(n=800):
    """Generate product data"""
    products = []
    
    for i in range(1, n + 1):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        subcategory = random.choice(PRODUCT_CATEGORIES[category])
        brand = random.choice(BRANDS)
        
        # Generate realistic Nepal pricing (in NPR)
        if category == 'Electronics':
            price = random.randint(5000, 150000)
        elif category == 'Fashion':
            price = random.randint(500, 15000)
        elif category == 'Home & Kitchen':
            price = random.randint(1000, 25000)
        elif category == 'Books':
            price = random.randint(200, 2000)
        elif category == 'Health & Beauty':
            price = random.randint(300, 3000)
        elif category == 'Sports':
            price = random.randint(800, 12000)
        else:  # Groceries
            price = random.randint(50, 1500)
        
        cost = price * random.uniform(0.4, 0.7)  # 40-70% margin
        
        products.append({
            'product_id': i,
            'name': f"{brand} {subcategory}",
            'category': category,
            'subcategory': subcategory,
            'brand': brand,
            'price': round(price, 2),
            'cost': round(cost, 2),
            'weight_kg': round(random.uniform(0.1, 5.0), 2),
            'created_at': fake.date_time_between(start_date='-1y', end_date='now')
        })
    
    return products

def generate_orders(n=50000, customer_count=12000):
    """Generate order data"""
    orders = []
    
    for i in range(1, n + 1):
        customer_id = random.randint(1, customer_count)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        # Location ID from seed file (1-8)
        shipping_address_id = random.randint(1, 8)
        
        # Weighted status selection
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
        
        # Generate realistic order amounts (NPR)
        base_amount = random.randint(500, 25000)
        shipping_cost = random.choice([0, 100, 150, 200])  # Free shipping or standard rates
        discount_amount = base_amount * random.uniform(0, 0.15) if random.random() < 0.3 else 0
        
        total_amount = base_amount + shipping_cost - discount_amount
        
        orders.append({
            'order_id': i,
            'customer_id': customer_id,
            'order_date': order_date,
            'status': status,
            'total_amount': round(total_amount, 2),
            'shipping_cost': shipping_cost,
            'discount_amount': round(discount_amount, 2),
            'shipping_address_id': shipping_address_id,
            'created_at': order_date,
            'updated_at': order_date + timedelta(days=random.randint(0, 7))
        })
    
    return orders

def generate_order_items(orders, product_count=800):
    """Generate order items data"""
    order_items = []
    item_id = 1
    
    for order in orders:
        # Random number of items per order (1-4, weighted toward 1-2)
        num_items = random.choices([1, 2, 3, 4], weights=[0.4, 0.35, 0.2, 0.05])[0]
        
        selected_products = random.sample(range(1, product_count + 1), num_items)
        
        for product_id in selected_products:
            quantity = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
            
            # Generate unit price (slight variation from product price)
            base_price = random.randint(500, 15000)  # Simplified for demo
            unit_price = base_price * random.uniform(0.9, 1.1)
            
            item_discount = unit_price * quantity * random.uniform(0, 0.1) if random.random() < 0.2 else 0
            total_price = (unit_price * quantity) - item_discount
            
            order_items.append({
                'order_item_id': item_id,
                'order_id': order['order_id'],
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': round(unit_price, 2),
                'total_price': round(total_price, 2),
                'discount_amount': round(item_discount, 2)
            })
            
            item_id += 1
    
    return order_items

def generate_payments(orders):
    """Generate payment data"""
    payments = []
    
    for order in orders:
        payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
        payment_status = random.choices(PAYMENT_STATUSES, weights=PAYMENT_STATUS_WEIGHTS)[0]
        
        # Payment date usually same as order date or shortly after
        payment_date = order['order_date'] + timedelta(
            minutes=random.randint(0, 1440)  # Within 24 hours
        )
        
        # Transaction ID for digital payments
        transaction_id = None
        if payment_method in ['eSewa', 'Khalti', 'IME Pay', 'Credit Card', 'Debit Card']:
            transaction_id = f"{payment_method[:3].upper()}{random.randint(100000, 999999)}"
        
        payments.append({
            'payment_id': order['order_id'],  # 1:1 relationship
            'order_id': order['order_id'],
            'payment_method': payment_method,
            'amount': order['total_amount'],
            'payment_date': payment_date,
            'status': payment_status,
            'transaction_id': transaction_id
        })
    
    return payments

def create_database_and_tables():
    """Create DuckDB database and insert all data"""
    
    print("Generating customer data...")
    customers = generate_customers(12000)
    
    print("Generating product data...")
    products = generate_products(800)
    
    print("Generating order data...")
    orders = generate_orders(50000, 12000)
    
    print("Generating order items data...")
    order_items = generate_order_items(orders, 800)
    
    print("Generating payment data...")
    payments = generate_payments(orders)
    
    print(f"Generated data summary:")
    print(f"- Customers: {len(customers):,}")
    print(f"- Products: {len(products):,}")
    print(f"- Orders: {len(orders):,}")
    print(f"- Order Items: {len(order_items):,}")
    print(f"- Payments: {len(payments):,}")
    
    # Create DuckDB connection
    conn = duckdb.connect('nepal_ecommerce.duckdb')
    
    print("\nCreating database tables...")
    
    # Create and populate customers table
    customers_df = pd.DataFrame(customers)
    conn.execute("DROP TABLE IF EXISTS customers")
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            date_of_birth DATE,
            gender VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    conn.execute("INSERT INTO customers SELECT * FROM customers_df")
    
    # Create and populate products table
    products_df = pd.DataFrame(products)
    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            brand VARCHAR,
            price DECIMAL(10,2),
            cost DECIMAL(10,2),
            weight_kg DECIMAL(5,2),
            created_at TIMESTAMP
        )
    """)
    conn.execute("INSERT INTO products SELECT * FROM products_df")
    
    # Create and populate orders table
    orders_df = pd.DataFrame(orders)
    conn.execute("DROP TABLE IF EXISTS orders")
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TIMESTAMP,
            status VARCHAR,
            total_amount DECIMAL(10,2),
            shipping_cost DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            shipping_address_id INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    conn.execute("INSERT INTO orders SELECT * FROM orders_df")
    
    # Create and populate order_items table
    order_items_df = pd.DataFrame(order_items)
    conn.execute("DROP TABLE IF EXISTS order_items")
    conn.execute("""
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_price DECIMAL(10,2),
            discount_amount DECIMAL(10,2)
        )
    """)
    conn.execute("INSERT INTO order_items SELECT * FROM order_items_df")
    
    # Create and populate payments table
    payments_df = pd.DataFrame(payments)
    conn.execute("DROP TABLE IF EXISTS payments")
    conn.execute("""
        CREATE TABLE payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            payment_method VARCHAR,
            amount DECIMAL(10,2),
            payment_date TIMESTAMP,
            status VARCHAR,
            transaction_id VARCHAR
        )
    """)
    conn.execute("INSERT INTO payments SELECT * FROM payments_df")
    
    print("Database created successfully!")
    
    # Show table counts
    print("\nTable row counts:")
    for table in ['customers', 'products', 'orders', 'order_items', 'payments']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"- {table}: {count:,}")
    
    # Optional: Export to CSV files for dbt seeds/testing
    print("\nExporting to CSV files...")
    customers_df.to_csv('customers.csv', index=False)
    products_df.to_csv('products.csv', index=False)
    orders_df.to_csv('orders.csv', index=False)
    order_items_df.to_csv('order_items.csv', index=False)
    payments_df.to_csv('payments.csv', index=False)
    
    conn.close()
    print("All done! Database saved as 'nepal_ecommerce.duckdb'")

if __name__ == "__main__":
    create_database_and_tables()