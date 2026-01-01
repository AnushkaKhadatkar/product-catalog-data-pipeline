import pandas as pd
from datetime import datetime
import random

print("=" * 60)
print("TESTING SCD TYPE 2 - MODIFYING PRODUCT DATA")
print("=" * 60)

# Read the prepared data
csv_path = '/Users/simmi/hw3.2_pipeline/data/data/products_prepared.csv'
df = pd.read_csv(csv_path)

print(f"\nOriginal dataset: {len(df)} products")

# Select 20 random products to modify
sample_size = 20
random.seed(42)
indices_to_modify = random.sample(range(len(df)), sample_size)

print(f"\nModifying {sample_size} random products...")
print("\nCHANGES:")
print("-" * 60)

for idx in indices_to_modify:
    product_id = df.loc[idx, 'product_id']
    old_price = df.loc[idx, 'price']
    old_title = df.loc[idx, 'title']
    
    # Increase prices by 10-30%
    price_multiplier = random.uniform(1.10, 1.30)
    new_price = round(old_price * price_multiplier, 2)
    df.loc[idx, 'price'] = new_price
    
    # Update some titles
    if random.random() > 0.5:
        df.loc[idx, 'title'] = f"Updated - {old_title}"
    
    # Update timestamp
    df.loc[idx, 'updated_at'] = datetime.now()
    
    change_pct = round((new_price - old_price) / old_price * 100, 1)
    print(f"Product {product_id}: ${old_price:.2f} → ${new_price:.2f} ({change_pct:+.1f}%)")

# Save modified data
df.to_csv(csv_path, index=False)

print("\n" + "=" * 60)
print(f"✓ Successfully modified {sample_size} products!")
print(f"✓ Updated file: {csv_path}")
print("\n✓ Ready to re-run pipeline to test SCD Type 2!")
print("=" * 60)