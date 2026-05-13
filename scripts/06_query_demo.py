import sys
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce_recs"


def main():
    if len(sys.argv) != 3:
        print("Usage:")
        print("python 06_query_demo.py <user_id> <item_id>")
        print()
        print("Example:")
        print("python 06_query_demo.py User_0 ITEM_1911")
        sys.exit(1)

    user_id = sys.argv[1]
    item_id = sys.argv[2]

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    user_profile = db.user_profiles.find_one(
        {"user_id": user_id},
        {"_id": 0}
    )

    product = db.product_catalog.find_one(
        {"item_id": item_id},
        {"_id": 0}
    )

    print("=" * 60)
    print("E-Commerce Recommendation Query Demo")
    print("=" * 60)

    print(f"User ID: {user_id}")
    print(f"Item ID: {item_id}")

    if not user_profile:
        print("\nResult: User profile not found.")
        client.close()
        return

    if not product:
        print("\nResult: Product not found.")
        client.close()
        return

    item_category = product.get("category")
    item_brand = product.get("brand")

    top_categories = user_profile.get("top_categories", [])
    top_category_names = [
        item.get("category")
        for item in top_categories
        if item.get("category")
    ]

    if item_category in top_category_names:
        campaign_type = "High_Discount"
    else:
        campaign_type = "Standard_Reminder"

    print("\nUser Top Categories:")
    for cat in top_categories:
        print(f"- {cat.get('category')} | score = {cat.get('score')}")

    print("\nProduct Info:")
    print(f"- Category: {item_category}")
    print(f"- Brand: {item_brand}")

    print("\nRecommendation Decision:")
    print(f"- Campaign Type: {campaign_type}")

    print("=" * 60)

    client.close()


if __name__ == "__main__":
    main()