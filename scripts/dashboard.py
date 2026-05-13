import streamlit as st
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce_recs"

st.set_page_config(page_title="E-Commerce Recommendation Demo", layout="wide")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

st.title("E-Commerce Behavioral Analytics & Recommendation Engine")

st.subheader("MongoDB Collection Counts")

col1, col2, col3 = st.columns(3)

col1.metric("User Profiles", db.user_profiles.count_documents({}))
col2.metric("Item Pairs", db.item_pairs.count_documents({}))
col3.metric("Product Catalog", db.product_catalog.count_documents({}))

st.divider()

st.subheader("Recommendation Query Demo")

user_id = st.text_input("User ID", "User_0")
item_id = st.text_input("Item ID", "ITEM_1911")

if st.button("Run Recommendation"):
    user_profile = db.user_profiles.find_one({"user_id": user_id}, {"_id": 0})
    product = db.product_catalog.find_one({"item_id": item_id}, {"_id": 0})

    if not user_profile:
        st.error("User profile not found.")
    elif not product:
        st.error("Product not found.")
    else:
        top_categories = user_profile.get("top_categories", [])
        top_category_names = [x["category"] for x in top_categories]

        item_category = product.get("category")
        item_brand = product.get("brand")

        if item_category in top_category_names:
            campaign_type = "High_Discount"
            st.success(f"Campaign Type: {campaign_type}")
        else:
            campaign_type = "Standard_Reminder"
            st.info(f"Campaign Type: {campaign_type}")

        left, right = st.columns(2)

        with left:
            st.write("### User Profile")
            st.json(user_profile)

        with right:
            st.write("### Product Info")
            st.json(product)

client.close()