# E-Commerce Behavioral Analytics & Recommendation Engine

## 1. Project Overview

This project is an end-to-end Big Data Engineering pipeline for e-commerce behavioral analytics and recommendation-based marketing.

The dataset contains raw e-commerce web log events such as:

- `view`
- `cart`
- `purchase`

The main goal is to process a large behavioral dataset using Apache Spark, generate user and product insights, store results in MongoDB, and use those results to decide whether a user should receive a targeted cart-abandonment campaign.

The final system can answer questions like:

- Which products are purchased together?
- What are each user's favorite product categories?
- Which users abandoned cart items?
- Should an abandoned-cart user receive a `High_Discount` or `Standard_Reminder`?

---

## 2. Team Members

| Name | 

| Ahmed Elkhateeb
| Ahmed Sherif
| Seif Matar

---

## 3. Main Project Idea

The project simulates a data engineering workflow for an e-commerce company.

The company has a large raw log dataset containing user behavior. The data is too large to process safely using Pandas or normal Python loops, so we use Apache Spark.

The pipeline works like this:

```text
Raw E-Commerce Logs CSV
        |
        v
Apache Spark / PySpark
        |
        |-- Cleaning + JSON Metadata Extraction
        |
        |-- Market Basket Analysis
        |       Output: item_pairs
        |
        |-- User Affinity Aggregation
                Output: user_profiles
        |
        v
MongoDB NoSQL Database
        |
        |-- user_profiles
        |-- item_pairs
        |-- product_catalog
        |
        v
Spark Cart Abandonment Pipeline
        |
        |-- Detect cart events without matching purchase
        |-- Join abandoned cart items with user affinity profiles
        |
        v
Campaign Output
        |
        |-- High_Discount
        |-- Standard_Reminder
```

---

## 4. Tools and Technologies Used

| Tool | Purpose |
|---|---|
| Python 3.10.11 | Main programming language |
| Apache Spark / PySpark 3.5.8 | Distributed processing of the large dataset |
| Hadoop winutils | Required for Spark file writing on Windows |
| Docker Desktop | Runs MongoDB locally inside a container |
| MongoDB | NoSQL storage for user profiles, product catalog, and item pairs |
| MongoDB Compass | Visual interface for inspecting MongoDB collections |
| Jupyter Notebook | Visual explanation and result presentation |
| Streamlit | Interactive recommendation dashboard |
| VS Code | Code editor and notebook environment |
| Git / GitHub | Collaboration and version control |

---

## 5. What We Installed

Each teammate should install the following.

### 5.1 Python 3.10.11

Install Python 3.10.11.

During installation, make sure to check:

```text
Add Python to PATH
```

Check installation:

```bash
py -3.10 --version
```

Expected:

```text
Python 3.10.11
```

---

### 5.2 Java JDK

Spark requires Java.

We used Java JDK 17.

Check installation:

```bash
java -version
```

Expected output should show Java 17 or a compatible version.

---

### 5.3 Docker Desktop

Docker is used to run MongoDB locally.

After installing Docker Desktop, open it and make sure Docker is running.

Check Docker:

```bash
docker --version
docker ps
```

---

### 5.4 VS Code

VS Code is used for:

- Python scripts
- Jupyter notebooks
- Running terminal commands
- Viewing project files

Recommended VS Code extensions:

- Python
- Jupyter
- Docker
- MongoDB for VS Code

---

### 5.5 MongoDB Compass

MongoDB Compass is used to visually inspect the MongoDB database.

Connection string:

```text
mongodb://localhost:27017
```

---

### 5.6 Hadoop Windows Helper Files

On Windows, Spark may fail while writing Parquet or JSON files unless Hadoop helper files exist.

We used:

```text
C:\hadoop\bin\winutils.exe
C:\hadoop\bin\hadoop.dll
```

Environment variables:

```text
HADOOP_HOME = C:\hadoop
hadoop.home.dir = C:\hadoop
```

`C:\hadoop\bin` should also be added to PATH.

Check:

```bash
where winutils
```

Expected:

```text
C:\hadoop\bin\winutils.exe
```

---

## 6. Repository Structure

```text
BigData_Project2/
│
├── scripts/
│   ├── 00_inspect_dataset.py
│   ├── 01_clean_logs.py
│   ├── 02_market_basket.py
│   ├── 03_user_affinity.py
│   ├── 04_load_to_mongodb.py
│   ├── 04b_load_product_catalog.py
│   ├── 05_cart_abandonment.py
│   ├── 06_query_demo.py
│   └── dashboard.py
│
├── notebooks/
│   ├── 01_Project_Overview.ipynb
│   ├── 02_Phase1_Spark_Results.ipynb
│   ├── 03_MongoDB_Demo.ipynb
│   └── 04_Cart_Abandonment_Demo.ipynb
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

The following folders are ignored by Git because they are large or generated locally:

```text
data/
output/
logs/
.venv/
```

---

## 7. Dataset

The dataset is not included in GitHub because it is very large.

Each teammate must place the dataset manually in:

```text
data/raw_logs.csv
```

Expected raw columns:

| Column | Description |
|---|---|
| timestamp | Event timestamp |
| session_id | User session identifier |
| user_id | User/customer identifier |
| event_type | Event type: `view`, `cart`, or `purchase` |
| product_id | Product/item identifier |
| price | Product price |
| referrer | Traffic source |
| user_metadata | JSON field containing user device, tier, and location |
| product_metadata | JSON field containing product category, brand, and stock |

Raw dataset row count:

```text
10,831,817 rows
```

Raw event distribution:

```text
view      8,123,448
cart      2,166,961
purchase    541,408
```

Product categories found:

```text
Home
Books
Electronics
Toys
Clothing
```

After cleaning, the final cleaned dataset contained:

```text
9,748,839 rows
```

---

## 8. Setup Instructions for Teammates

### Step 1 — Clone the Repository

```bash
git clone https://github.com/AhmedElkhateeb11/ecommerce-behavioral-analytics.git
cd ecommerce-behavioral-analytics
```

---

### Step 2 — Create a Virtual Environment

```bash
py -3.10 -m venv .venv
```

Activate it on Windows CMD:

```bash
.venv\Scripts\activate.bat
```

If using PowerShell and activation is blocked, either use CMD or run scripts directly using:

```bash
.venv\Scripts\python.exe
```

---

### Step 3 — Install Python Dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Main packages used:

```text
pyspark
pymongo
streamlit
pandas
notebook
ipykernel
```

---

### Step 4 — Add the Dataset

Create a `data` folder:

```bash
mkdir data
```

Place the dataset here:

```text
data/raw_logs.csv
```

The dataset file is ignored by Git and should not be uploaded to GitHub.

---

### Step 5 — Start MongoDB Using Docker

Make sure Docker Desktop is running.

Then run:

```bash
docker compose up -d
```

Check container:

```bash
docker ps
```

Expected container name:

```text
ecommerce_mongodb
```

MongoDB connection string:

```text
mongodb://localhost:27017
```

---

## 9. Docker Compose

The project uses `docker-compose.yml` to run MongoDB.

```yaml
services:
  mongodb:
    image: mongo:7
    container_name: ecommerce_mongodb
    restart: always
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

volumes:
  mongo_data:
```

Start MongoDB:

```bash
docker compose up -d
```

Stop MongoDB:

```bash
docker compose down
```

---

# 10. Pipeline Execution

Run the scripts in the exact order below.

---

## Step 0 — Inspect Dataset

Script:

```bash
python scripts/00_inspect_dataset.py
```

Purpose:

- Read the raw CSV using Spark
- Print column names
- Print schema
- Show first rows
- Count total rows
- Count event types
- Confirm JSON parsing works

Important output from this step:

```text
Columns:
timestamp, session_id, user_id, event_type, product_id, price, referrer, user_metadata, product_metadata
```

Event types:

```text
view
cart
purchase
```

This step confirmed that `product_metadata` contains JSON like:

```json
{
  "category": "Clothing",
  "brand": "Zara",
  "stock": 55
}
```

---

## Step 1 — Clean Logs

Script:

```bash
python scripts/01_clean_logs.py
```

Purpose:

- Read raw CSV with Spark
- Extract JSON fields from `user_metadata`
- Extract JSON fields from `product_metadata`
- Convert timestamp to Spark timestamp type
- Rename `product_id` to `item_id`
- Extract:
  - category
  - brand
  - stock
  - device
  - user tier
  - user location
- Remove invalid rows
- Save the cleaned dataset as Parquet

Output folder:

```text
output/clean_logs_parquet/
```

Cleaned schema:

```text
event_timestamp
session_id
user_id
event_type
item_id
price
referrer
device
user_tier
user_location
category
brand
stock
```

Cleaned row count:

```text
9,748,839 rows
```

Why Parquet was used:

Parquet is faster and more efficient than CSV for Spark processing. After cleaning the raw CSV once, all later scripts read the Parquet output.

---

# 11. Phase 1 — Spark Distributed Processing

Phase 1 contains two Spark tasks:

1. Market Basket Analysis
2. User Affinity Aggregation

---

## 11.1 Task 1.1 — Market Basket Analysis

Script:

```bash
python scripts/02_market_basket.py
```

Objective:

Find items that are frequently purchased together in the same session.

MapReduce-style explanation:

```text
Map:
For each purchase event, emit:
(session_id, item_id)

Reduce:
Group items by session_id.
Generate all possible item pairs inside each session.
Count how many times each item pair appears globally.
```

Output folder:

```text
output/item_pairs/
```

Output columns:

```text
item_1
item_2
pair_count
```

Example output:

```text
item_1     item_2     pair_count
ITEM_3262  ITEM_857   2
ITEM_3264  ITEM_746   2
ITEM_386   ITEM_4168  2
```

Note:

The top pair counts are low because the dataset has many unique products and purchase sessions. This does not mean the logic failed. The script still correctly performs session-based item co-occurrence analysis.

---

## 11.2 Task 1.2 — User Affinity Aggregation

Script:

```bash
python scripts/03_user_affinity.py
```

Objective:

Build a behavioral profile for each user based on their actions.

Event scoring:

| Event Type | Score |
|---|---:|
| view | 1 |
| cart | 3 |
| purchase | 5 |

Logic:

```text
1. Read cleaned logs.
2. Ignore rows with missing category.
3. Convert event_type into a weighted score.
4. Group by user_id and category.
5. Sum scores.
6. Rank categories per user.
7. Keep the top 3 categories.
8. Save user profiles as JSON.
```

Output folder:

```text
output/user_profiles/
```

Example output:

```json
{
  "user_id": "User_0",
  "top_categories": [
    {"category": "Home", "score": 150},
    {"category": "Clothing", "score": 124},
    {"category": "Books", "score": 113}
  ]
}
```

This profile means `User_0` has the strongest historical interest in Home, Clothing, and Books.

---

# 12. Phase 2 — NoSQL Data Modeling and Storage

MongoDB was selected because the project needs fast lookup of nested user profiles and product metadata.

The MongoDB database is:

```text
ecommerce_recs
```

Collections:

```text
user_profiles
item_pairs
product_catalog
```

---

## 12.1 Collection: user_profiles

Stores the output of User Affinity Aggregation.

Example document:

```json
{
  "user_id": "User_0",
  "top_categories": [
    {"category": "Home", "score": 150},
    {"category": "Clothing", "score": 124},
    {"category": "Books", "score": 113}
  ]
}
```

Document count:

```text
25,000
```

Index:

```text
user_id
```

Purpose:

This collection is used to quickly retrieve a user's top categories during recommendation and cart-abandonment targeting.

---

## 12.2 Collection: item_pairs

Stores Market Basket Analysis results.

Example document:

```json
{
  "item_1": "ITEM_3262",
  "item_2": "ITEM_857",
  "pair_count": 2
}
```

Document count loaded:

```text
50,000
```

Indexes:

```text
item_1
item_2
item_1 + item_2
```

Purpose:

This collection stores products that appeared together in purchase sessions.

---

## 12.3 Collection: product_catalog

Stores product metadata extracted from the logs.

Example document:

```json
{
  "item_id": "ITEM_1911",
  "category": "Clothing",
  "brand": "Zara"
}
```

Index:

```text
item_id
```

Purpose:

This collection allows the query demo and campaign logic to look up the category and brand of a product.

---

## 12.4 Load Spark Outputs into MongoDB

Script:

```bash
python scripts/04_load_to_mongodb.py
```

Purpose:

- Load `user_profiles` into MongoDB
- Load top `item_pairs` into MongoDB
- Create indexes for faster lookup

Expected MongoDB counts:

```text
user_profiles: 25000
item_pairs: 50000
```

Important implementation note:

The first MongoDB loading version used Spark `foreachPartition`, but that caused Python worker crashes on Windows. The final version uses a safer approach:

1. Spark writes outputs to JSON.
2. Normal Python + PyMongo streams JSON into MongoDB.

This is more stable on Windows.

---

## 12.5 Load Product Catalog

Script:

```bash
python scripts/04b_load_product_catalog.py
```

Purpose:

- Read cleaned Parquet logs
- Extract unique `item_id`, `category`, and `brand`
- Save product catalog as JSON
- Load product catalog into MongoDB
- Create an index on `item_id`

---

# 13. Phase 3 — Cart Abandonment Recovery

Script:

```bash
python scripts/05_cart_abandonment.py
```

Objective:

Detect users who added an item to cart but did not purchase that same item in the same session.

Logic:

```text
1. Read cleaned logs with Spark.
2. Extract cart events.
3. Extract purchase events.
4. Use a left anti-join to find cart events without matching purchases.
5. Export MongoDB user profiles to Spark-readable JSON.
6. Read those profiles into Spark.
7. Join abandoned cart events with user top categories.
8. If abandoned item category is in the user's top categories:
      High_Discount
   Otherwise:
      Standard_Reminder
9. Save campaign output as Parquet.
```

Output folder:

```text
output/campaign_output/
```

Campaign result counts:

```text
Standard_Reminder: 67,842
High_Discount: 132,153
```

Example campaign output:

```text
user_id  session_id       item_id    category     campaign_type
User_0   SESS_01b30b5c1f  ITEM_1911  Clothing     High_Discount
User_0   SESS_0085eddbbc  ITEM_1618  Electronics  Standard_Reminder
```

Explanation:

`User_0` has top categories:

```text
Home
Clothing
Books
```

So:

```text
ITEM_1911 → Clothing → High_Discount
ITEM_1618 → Electronics → Standard_Reminder
```

---

# 14. Query Demo

Script:

```bash
python scripts/06_query_demo.py User_0 ITEM_1911
```

Purpose:

Demonstrate instant NoSQL lookup for one user and one item.

The script:

1. Connects to MongoDB.
2. Looks up the user profile from `user_profiles`.
3. Looks up the item category from `product_catalog`.
4. Compares item category with user's top categories.
5. Prints the campaign decision.

Example 1:

```bash
python scripts/06_query_demo.py User_0 ITEM_1911
```

Output:

```text
User ID: User_0
Item ID: ITEM_1911

User Top Categories:
- Home | score = 150
- Clothing | score = 124
- Books | score = 113

Product Info:
- Category: Clothing
- Brand: Zara

Recommendation Decision:
- Campaign Type: High_Discount
```

Example 2:

```bash
python scripts/06_query_demo.py User_0 ITEM_1618
```

Output:

```text
User ID: User_0
Item ID: ITEM_1618

User Top Categories:
- Home | score = 150
- Clothing | score = 124
- Books | score = 113

Product Info:
- Category: Electronics
- Brand: Dell

Recommendation Decision:
- Campaign Type: Standard_Reminder
```

---

# 15. Streamlit Dashboard

Script:

```bash
python -m streamlit run scripts/dashboard.py
```

Purpose:

Provide an interactive visual demo for discussion.

The dashboard shows:

- MongoDB collection counts
- Input for `user_id`
- Input for `item_id`
- User profile JSON
- Product info JSON
- Final campaign decision

Demo inputs:

```text
User_0 + ITEM_1911 → High_Discount
User_0 + ITEM_1618 → Standard_Reminder
```

This dashboard is useful during presentation because it avoids showing only command-line output.

---

# 16. Jupyter Notebooks

Four notebooks were created for visual presentation.

---

## 16.1 Notebook 1 — Project Overview

File:

```text
notebooks/01_Project_Overview.ipynb
```

Purpose:

Explains:

- Project goal
- Dataset structure
- Tools used
- Pipeline architecture
- Why Spark and MongoDB are used

This notebook is mainly markdown and is used at the start of the discussion.

---

## 16.2 Notebook 2 — Phase 1 Spark Results

File:

```text
notebooks/02_Phase1_Spark_Results.ipynb
```

Purpose:

Shows Spark results from Phase 1.

It displays:

- Cleaned dataset schema
- Sample cleaned rows
- Event type counts
- Category counts
- Top market basket item pairs
- Sample user affinity profiles

This notebook reads generated outputs from:

```text
output/clean_logs_parquet/
output/item_pairs/
output/user_profiles/
```

It does not rerun the full heavy pipeline.

---

## 16.3 Notebook 3 — MongoDB Demo

File:

```text
notebooks/03_MongoDB_Demo.ipynb
```

Purpose:

Shows MongoDB collections and sample documents.

It displays:

- Collection counts
- Sample `user_profiles` document
- Sample `product_catalog` documents
- Sample `item_pairs` documents
- MongoDB indexes

This notebook connects to:

```text
mongodb://localhost:27017
```

Database:

```text
ecommerce_recs
```

---

## 16.4 Notebook 4 — Cart Abandonment Demo

File:

```text
notebooks/04_Cart_Abandonment_Demo.ipynb
```

Purpose:

Shows final Phase 3 campaign results.

It displays:

- Sample campaign output
- Count of `High_Discount`
- Count of `Standard_Reminder`
- Query demo logic
- Example decision for `User_0 + ITEM_1911`
- Example decision for `User_0 + ITEM_1618`

---

# 17. MongoDB Compass Visual Demo

MongoDB Compass is used to visually inspect the database.

Connection:

```text
mongodb://localhost:27017
```

Database:

```text
ecommerce_recs
```

Collections:

```text
user_profiles
item_pairs
product_catalog
```

Useful Compass filters:

Find one user profile:

```json
{ "user_id": "User_0" }
```

Find product `ITEM_1911`:

```json
{ "item_id": "ITEM_1911" }
```

Find product `ITEM_1618`:

```json
{ "item_id": "ITEM_1618" }
```

Sort item pairs by highest frequency:

```json
{ "pair_count": -1 }
```

Compass is useful for showing that MongoDB contains the actual NoSQL documents used by the pipeline.

---

# 18. Full Script Run Order

For a full rerun from scratch, use this order:

```bash
docker compose up -d

python scripts/00_inspect_dataset.py
python scripts/01_clean_logs.py
python scripts/02_market_basket.py
python scripts/03_user_affinity.py
python scripts/04_load_to_mongodb.py
python scripts/04b_load_product_catalog.py
python scripts/05_cart_abandonment.py
python scripts/06_query_demo.py User_0 ITEM_1911
python scripts/06_query_demo.py User_0 ITEM_1618
python -m streamlit run scripts/dashboard.py
```

---

# 19. Expected Final Results

| Component | Result |
|---|---:|
| Raw dataset rows | 10,831,817 |
| Cleaned dataset rows | 9,748,839 |
| MongoDB user profiles | 25,000 |
| MongoDB item pairs | 50,000 |
| Standard reminders | 67,842 |
| High discount campaigns | 132,153 |

---

# 20. Presentation Flow

Recommended demo order:

1. Show the architecture diagram.
2. Show VS Code project folder.
3. Explain the scripts.
4. Open `01_Project_Overview.ipynb`.
5. Open `02_Phase1_Spark_Results.ipynb`.
6. Show cleaned data, event counts, market basket output, and user profiles.
7. Open MongoDB Compass.
8. Show:
   - `user_profiles`
   - `item_pairs`
   - `product_catalog`
9. Open `04_Cart_Abandonment_Demo.ipynb`.
10. Show campaign type counts.
11. Open Streamlit dashboard.
12. Test:
   - `User_0`, `ITEM_1911`
   - `User_0`, `ITEM_1618`

---

# 21. Important Notes

## Why Spark was used

The dataset is too large for normal in-memory processing with Pandas. Spark was used to process the data using distributed transformations and MapReduce-style logic.

## Why MongoDB was used

MongoDB was selected because the project requires fast lookup of nested user profiles and product metadata. A document model is suitable for storing user profiles with embedded arrays of top categories.

## Why the dataset is not uploaded to GitHub

The raw dataset is over 2 GB, which is too large for GitHub. Each teammate should place the dataset locally inside:

```text
data/raw_logs.csv
```

## Why output folders are not uploaded

The `output/` folder is generated by Spark and may be large. It can be regenerated by running the pipeline scripts.

## Why notebooks are included

The notebooks are included because they make the project easier to present visually. They show the results without requiring the full pipeline to be rerun during discussion.

---

# 22. Troubleshooting

## Problem: PowerShell blocks virtual environment activation

Error:

```text
running scripts is disabled on this system
```

Fix:

Use CMD instead:

```bash
.venv\Scripts\activate.bat
```

Or run scripts directly:

```bash
.venv\Scripts\python.exe scripts/01_clean_logs.py
```

---

## Problem: Spark says winutils.exe is missing

Error:

```text
HADOOP_HOME and hadoop.home.dir are unset
```

Fix:

Make sure these files exist:

```text
C:\hadoop\bin\winutils.exe
C:\hadoop\bin\hadoop.dll
```

Environment variables:

```text
HADOOP_HOME = C:\hadoop
hadoop.home.dir = C:\hadoop
```

Check:

```bash
where winutils
```

---

## Problem: MongoDB connection fails

Make sure Docker is running:

```bash
docker ps
```

If MongoDB is not running:

```bash
docker compose up -d
```

---

## Problem: Dataset file not found

Make sure the dataset is located at:

```text
data/raw_logs.csv
```

---

## Problem: GitHub rejects large files

Do not upload:

```text
data/
output/
.venv/
logs/
```

These are ignored in `.gitignore`.

If they were accidentally added before `.gitignore`, reset Git tracking:

```bash
rmdir /s /q .git
git init
git add .
git status
git commit -m "Initial big data engineering project"
git branch -M main
git remote add origin https://github.com/AhmedElkhateeb11/ecommerce-behavioral-analytics.git
git push -u origin main
```

Before pushing, always check:

```bash
git status
```

Make sure `data/`, `output/`, and `.venv/` are not listed.

---

# 23. Limitations

- The project runs locally using Spark local mode, not a real multi-node Spark cluster.
- MongoDB is run locally through Docker.
- Phase 3 uses a batch limit for stable local execution.
- Market basket pair counts are low because the dataset contains many unique item combinations.
- The dataset and generated Spark outputs are excluded from GitHub due to size.

---

# 24. Conclusion

This project successfully implemented an end-to-end Big Data Engineering pipeline for e-commerce behavioral analytics.

Apache Spark was used to process millions of raw behavioral log records, clean JSON metadata, generate market basket item pairs, and calculate weighted user affinity profiles.

MongoDB was used as a NoSQL storage layer for fast lookup of user and product information.

The final Spark enrichment pipeline detected abandoned cart events and assigned campaign decisions based on whether the abandoned product category matched the user's historical affinity profile.

The project also includes visual presentation tools such as Jupyter Notebooks, MongoDB Compass, and a Streamlit dashboard to make the pipeline easier to explain and demonstrate.
