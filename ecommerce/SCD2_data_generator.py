import duckdb
import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Configuration
UPDATE_BATCH_SIZE = 1000  # Number of records to update per batch
NEW_RECORDS_BATCH_SIZE = 500  # Number of new records to add per batch

# Nepal-specific data (same as original script)
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

BRANDS = [
    'Samsung', 'Apple', 'Xiaomi', 'Dell', 'HP', 'Sony', 'LG', 'Panasonic',
    'Nike', 'Adidas', 'Puma', 'Zara', 'H&M', 'Local Brand', 'Generic',
    'Dabur', 'Himalaya', 'Unilever', 'P&G', 'Goldstar', 'CG', 'Bajaj'
]

PRODUCT_CATEGORIES = {
    'Electronics': ['Mobile Phone', 'Laptop', 'Headphones', 'Speaker', 'Charger', 'Power Bank', 'Camera'],
    'Fashion': ['T-Shirt', 'Jeans', 'Kurta', 'Saree', 'Shoes', 'Sandals', 'Bag', 'Watch'],
    'Home & Kitchen': ['Rice Cooker', 'Blender', 'Pressure Cooker', 'Thermos', 'Dinner Set', 'Curtain'],
    'Books': ['Novel', 'Textbook', 'Dictionary', 'Magazine', 'Comic Book'],
    'Health & Beauty': ['Face Cream', 'Shampoo', 'Soap', 'Toothpaste', 'Perfume', 'Makeup Kit'],
    'Sports': ['Football', 'Cricket Bat', 'Badminton Racket', 'Running Shoes', 'Yoga Mat'],
    'Groceries': ['Rice', 'Dal', 'Oil', 'Spices', 'Tea', 'Biscuits', 'Noodles']
}

ORDER_STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']
PAYMENT_METHODS = ['eSewa', 'Khalti', 'IME Pay', 'Cash on Delivery', 'Bank Transfer', 'Credit Card', 'Debit Card']

class SCD2DataGenerator:
    def __init__(self, db_path='nepal_ecommerce.duckdb'):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        
    def get_table_max_id(self, table_name, id_column):
        """Get the maximum ID from a table"""
        result = self.conn.execute(f"SELECT MAX({id_column}) FROM {table_name}").fetchone()
        return result[0] if result[0] is not None else 0
    
    def update_customer_data(self, update_timestamp=None):
        """Generate customer updates for SCD2 testing"""
        if update_timestamp is None:
            update_timestamp = datetime.now()
            
        print(f"Generating customer updates for timestamp: {update_timestamp}")
        
        # Get random customers to update
        customers = self.conn.execute(f"""
            SELECT customer_id, first_name, last_name, email, phone, gender
            FROM customers 
            ORDER BY RANDOM() 
            LIMIT {UPDATE_BATCH_SIZE}
        """).fetchall()
        
        updates = []
        for customer in customers:
            customer_id, first_name, last_name, email, phone, gender = customer
            
            # Randomly decide what to update (multiple changes possible)
            update_fields = {}
            
            # 30% chance to update phone number
            if random.random() < 0.3:
                update_fields['phone'] = f"+977-{random.randint(980, 989)}-{random.randint(1000000, 9999999)}"
            
            # 20% chance to update email
            if random.random() < 0.2:
                email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
                update_fields['email'] = f"{first_name.lower()}.{last_name.lower()}{random.randint(100, 999)}@{email_domain}"
            
            # 10% chance to update first name (marriage, etc.)
            if random.random() < 0.1:
                update_fields['first_name'] = random.choice(NEPAL_FIRST_NAMES)
            
            # 5% chance to update last name (marriage)
            if random.random() < 0.05:
                update_fields['last_name'] = random.choice(NEPAL_LAST_NAMES)
            
            if update_fields:
                update_fields['customer_id'] = customer_id
                update_fields['updated_at'] = update_timestamp
                updates.append(update_fields)
        
        if updates:
            # Create temporary table with updates
            updates_df = pd.DataFrame(updates)
            self.conn.execute("DROP TABLE IF EXISTS temp_customer_updates")
            self.conn.execute("""
                CREATE TABLE temp_customer_updates AS 
                SELECT * FROM updates_df
            """)
            
            # Apply updates
            update_sql = """
                UPDATE customers 
                SET first_name = COALESCE(temp.first_name, customers.first_name),
                    last_name = COALESCE(temp.last_name, customers.last_name),
                    email = COALESCE(temp.email, customers.email),
                    phone = COALESCE(temp.phone, customers.phone),
                    updated_at = temp.updated_at
                FROM temp_customer_updates temp
                WHERE customers.customer_id = temp.customer_id
            """
            self.conn.execute(update_sql)
            self.conn.execute("DROP TABLE temp_customer_updates")
            
            print(f"Updated {len(updates)} customer records")
        
        return len(updates)
    
    def update_product_data(self, update_timestamp=None):
        """Generate product updates for SCD2 testing"""
        if update_timestamp is None:
            update_timestamp = datetime.now()
            
        print(f"Generating product updates for timestamp: {update_timestamp}")
        
        # Get random products to update
        products = self.conn.execute(f"""
            SELECT product_id, name, category, price, cost
            FROM products 
            ORDER BY RANDOM() 
            LIMIT {UPDATE_BATCH_SIZE}
        """).fetchall()
        
        updates = []
        for product in products:
            product_id, name, category, price, cost = product
            
            update_fields = {}
            
            # 50% chance to update price (common in e-commerce)
            if random.random() < 0.5:
                # Price change between -20% to +30%
                price_change = random.uniform(-0.2, 0.3)
                new_price = price * (1 + price_change)
                update_fields['price'] = round(max(new_price, 50), 2)  # Minimum price 50 NPR
            
            # 30% chance to update cost (supplier changes)
            if random.random() < 0.3:
                cost_change = random.uniform(-0.15, 0.25)
                new_cost = cost * (1 + cost_change)
                update_fields['cost'] = round(max(new_cost, 20), 2)  # Minimum cost 20 NPR
            
            # 10% chance to update category/subcategory
            if random.random() < 0.1:
                new_category = random.choice(list(PRODUCT_CATEGORIES.keys()))
                new_subcategory = random.choice(PRODUCT_CATEGORIES[new_category])
                update_fields['category'] = new_category
                update_fields['subcategory'] = new_subcategory
            
            # 15% chance to update brand
            if random.random() < 0.15:
                update_fields['brand'] = random.choice(BRANDS)
            
            if update_fields:
                update_fields['product_id'] = product_id
                updates.append(update_fields)
        
        if updates:
            updates_df = pd.DataFrame(updates)
            self.conn.execute("DROP TABLE IF EXISTS temp_product_updates")
            self.conn.execute("CREATE TABLE temp_product_updates AS SELECT * FROM updates_df")
            
            update_sql = """
                UPDATE products 
                SET price = COALESCE(temp.price, products.price),
                    cost = COALESCE(temp.cost, products.cost),
                    category = COALESCE(temp.category, products.category),
                    subcategory = COALESCE(temp.subcategory, products.subcategory),
                    brand = COALESCE(temp.brand, products.brand)
                FROM temp_product_updates temp
                WHERE products.product_id = temp.product_id
            """
            self.conn.execute(update_sql)
            self.conn.execute("DROP TABLE temp_product_updates")
            
            print(f"Updated {len(updates)} product records")
        
        return len(updates)
    
    def add_new_customers(self, add_timestamp=None):
        """Add new customers"""
        if add_timestamp is None:
            add_timestamp = datetime.now()
            
        max_customer_id = self.get_table_max_id('customers', 'customer_id')
        new_customers = []
        
        for i in range(NEW_RECORDS_BATCH_SIZE):
            customer_id = max_customer_id + i + 1
            first_name = random.choice(NEPAL_FIRST_NAMES)
            last_name = random.choice(NEPAL_LAST_NAMES)
            
            email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{email_domain}"
            phone = f"+977-{random.randint(980, 989)}-{random.randint(1000000, 9999999)}"
            birth_date = fake.date_of_birth(minimum_age=18, maximum_age=70)
            
            new_customers.append({
                'customer_id': customer_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'date_of_birth': birth_date,
                'gender': random.choice(['Male', 'Female']),
                'created_at': add_timestamp,
                'updated_at': add_timestamp
            })
        
        customers_df = pd.DataFrame(new_customers)
        self.conn.execute("INSERT INTO customers SELECT * FROM customers_df")
        print(f"Added {len(new_customers)} new customers")
        
        return len(new_customers)
    
    def add_new_products(self, add_timestamp=None):
        """Add new products"""
        if add_timestamp is None:
            add_timestamp = datetime.now()
            
        max_product_id = self.get_table_max_id('products', 'product_id')
        new_products = []
        
        for i in range(NEW_RECORDS_BATCH_SIZE // 2):  # Fewer new products than customers
            product_id = max_product_id + i + 1
            category = random.choice(list(PRODUCT_CATEGORIES.keys()))
            subcategory = random.choice(PRODUCT_CATEGORIES[category])
            brand = random.choice(BRANDS)
            
            # Generate realistic pricing
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
            
            cost = price * random.uniform(0.4, 0.7)
            
            new_products.append({
                'product_id': product_id,
                'name': f"{brand} {subcategory}",
                'category': category,
                'subcategory': subcategory,
                'brand': brand,
                'price': round(price, 2),
                'cost': round(cost, 2),
                'weight_kg': round(random.uniform(0.1, 5.0), 2),
                'created_at': add_timestamp
            })
        
        products_df = pd.DataFrame(new_products)
        self.conn.execute("INSERT INTO products SELECT * FROM products_df")
        print(f"Added {len(new_products)} new products")
        
        return len(new_products)
    
    def update_order_statuses(self, update_timestamp=None):
        """Update order statuses (common business operation)"""
        if update_timestamp is None:
            update_timestamp = datetime.now()
            
        # Update orders that are in transitional states
        orders_to_update = self.conn.execute("""
            SELECT order_id, status 
            FROM orders 
            WHERE status IN ('pending', 'confirmed', 'shipped')
            ORDER BY RANDOM()
            LIMIT ?
        """, [UPDATE_BATCH_SIZE // 2]).fetchall()
        
        updates = []
        for order_id, current_status in orders_to_update:
            # Define status transitions
            if current_status == 'pending' and random.random() < 0.7:
                new_status = random.choice(['confirmed', 'cancelled'])
            elif current_status == 'confirmed' and random.random() < 0.8:
                new_status = 'shipped'
            elif current_status == 'shipped' and random.random() < 0.9:
                new_status = 'delivered'
            else:
                continue
                
            updates.append({
                'order_id': order_id,
                'status': new_status,
                'updated_at': update_timestamp
            })
        
        if updates:
            updates_df = pd.DataFrame(updates)
            self.conn.execute("DROP TABLE IF EXISTS temp_order_updates")
            self.conn.execute("CREATE TABLE temp_order_updates AS SELECT * FROM updates_df")
            
            self.conn.execute("""
                UPDATE orders 
                SET status = temp.status,
                    updated_at = temp.updated_at
                FROM temp_order_updates temp
                WHERE orders.order_id = temp.order_id
            """)
            self.conn.execute("DROP TABLE temp_order_updates")
            
            print(f"Updated {len(updates)} order statuses")
        
        return len(updates)
    
    def generate_incremental_batch(self, batch_timestamp=None):
        """Generate a complete batch of incremental updates"""
        if batch_timestamp is None:
            batch_timestamp = datetime.now()
            
        print(f"\n=== Generating incremental batch for {batch_timestamp} ===")
        
        total_changes = 0
        
        # Update existing data
        total_changes += self.update_customer_data(batch_timestamp)
        total_changes += self.update_product_data(batch_timestamp)
        total_changes += self.update_order_statuses(batch_timestamp)
        
        # Add new data
        total_changes += self.add_new_customers(batch_timestamp)
        total_changes += self.add_new_products(batch_timestamp)
        
        print(f"Total changes in this batch: {total_changes}")
        return total_changes
    
    def generate_historical_batches(self, num_batches=5, days_between_batches=7):
        """Generate multiple historical batches for testing"""
        print(f"Generating {num_batches} historical batches...")
        
        base_date = datetime.now() - timedelta(days=num_batches * days_between_batches)
        
        for i in range(num_batches):
            batch_date = base_date + timedelta(days=i * days_between_batches)
            self.generate_incremental_batch(batch_date)
            
        print(f"\nCompleted generating {num_batches} historical batches")
    
    def get_summary_stats(self):
        """Print summary statistics"""
        print("\n=== Current Database Summary ===")
        
        tables = ['customers', 'products', 'orders', 'order_items', 'payments']
        for table in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table}: {count:,} records")
            
        # Show recent updates
        print("\n=== Recent Updates ===")
        recent_customers = self.conn.execute("""
            SELECT COUNT(*) FROM customers 
            WHERE updated_at > created_at
        """).fetchone()[0]
        print(f"Customers with updates: {recent_customers:,}")
        
    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    """Main function to demonstrate usage"""
    generator = SCD2DataGenerator()
    
    print("SCD2 Data Generator for Nepal E-commerce Database")
    print("=" * 50)
    
    # Show current state
    generator.get_summary_stats()
    
    # Generate a single incremental batch
    print("\nOption 1: Generate single incremental batch")
    print("Option 2: Generate multiple historical batches")
    print("Option 3: Exit")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == '1':
        generator.generate_incremental_batch()
        generator.get_summary_stats()
        
    elif choice == '2':
        num_batches = int(input("Enter number of batches to generate (default 5): ") or "5")
        days_between = int(input("Enter days between batches (default 7): ") or "7")
        generator.generate_historical_batches(num_batches, days_between)
        generator.get_summary_stats()
        
    elif choice == '3':
        print("Exiting...")
    else:
        print("Invalid choice")
    
    generator.close()
    print("Done!")

if __name__ == "__main__":
    main()