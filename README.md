[README.md](https://github.com/user-attachments/files/32009950/README.md)
# 🛒 Real-Time E-Commerce Analytics Pipeline

A complete open-source project demonstrating a real-time big data pipeline using Apache Kafka, Apache Spark Structured Streaming, and Streamlit. This project simulates live e-commerce user clicks, streams them through Kafka, processes the aggregates in real-time with PySpark, and visualizes the results on a live updating dashboard.

## 🏗️ Architecture
**Python Script (Fake Data)** ➔ **Kafka Topic (clickstream)** ➔ **Spark Structured Streaming** ➔ **Streamlit Dashboard**

## 📋 Prerequisites
- Ubuntu VM
- Java 17 or higher (Required for Kafka KRaft mode and Spark)
- Python 3 installed (`sudo apt install python3 python3-pip python3-venv`)

---

## 🚀 Step 1: Installing and Starting Kafka (KRaft Mode)
*Note: This uses the modern KRaft mode, meaning Zookeeper is no longer required.*

1. **Download and Extract Kafka:**
```bash
wget https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz
cd kafka_2.13-4.3.1
```

2. **Start Kafka Native (KRaft):**
Open a terminal inside the Kafka directory and run:
```bash
# Generate a Cluster UUID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Format Log Directories
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

# Start the Kafka Server
bin/kafka-server-start.sh config/server.properties
```

3. **Create the Kafka Topic:**
Open a **new terminal** (stay inside the kafka directory) and run:
```bash
bin/kafka-topics.sh --create --topic clickstream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## 🐍 Step 2: Setup Python Virtual Environment
To avoid system environment restrictions in Ubuntu, set up a virtual environment.

Open a new terminal in your main project folder (e.g., `~/Documents/ecommerce_project`):
```bash
# Create the virtual environment
python3 -m venv kafka_env

# Activate the virtual environment
source kafka_env/bin/activate

# Install all required libraries
pip install kafka-python-ng pyspark streamlit pandas matplotlib
```
*Note: You must run `source kafka_env/bin/activate` in every new terminal where you plan to run Python scripts for this project.*

---

## 📤 Step 3: Real-Time Data Generator (Kafka Producer)
Create a file named `producer.py` and paste the following code:

```python
from kafka import KafkaProducer
import json
import time
import random

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['Laptop', 'Smartphone', 'Headphones', 'Smartwatch', 'Tablet']
actions = ['click', 'add_to_cart', 'purchase']

print("Starting to generate fake e-commerce data...")

while True:
    data = {
        "user_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "action": random.choice(actions),
        "timestamp": time.time()
    }
    producer.send('clickstream', value=data)
    print(f"Sent: {data}")
    time.sleep(1) # 1-second delay
```

---

## ⚙️ Step 4: Data Processing with Spark Streaming
Create a file named `spark_processor.py` and paste the following code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Create Spark Session
spark = SparkSession.builder \
    .appName("EcommerceRealTimeAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .load()

# Define JSON Schema
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", DoubleType())

# Parse binary value to JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggregate Data: Count interactions per product
click_counts = parsed_df.filter(col("action") == "click") \
    .groupBy("product") \
    .count()

# Output the aggregated counts to the console continuously
query = click_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

---

## 📊 Step 5: Real-Time Dashboard (Streamlit)
Create a file named `dashboard.py` and paste the following code:

```python
import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

st.set_page_config(page_title="E-Commerce Dashboard", page_icon="🛒", layout="wide")
st.title("🛒 Real-Time E-Commerce Analytics")

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'clickstream',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

placeholder = st.empty()
data_list = []

for message in consumer:
    data_list.append(message.value)
    if len(data_list) > 100: # Keep only the latest 100 records for memory efficiency
        data_list.pop(0)
        
    df = pd.DataFrame(data_list)
    
    with placeholder.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("### Live Clicks Stream (Latest)")
            st.dataframe(df.tail(10), use_container_width=True)
            
        with col2:
            if not df.empty:
                st.write("### Top Products by Interaction")
                product_counts = df['product'].value_counts()
                st.bar_chart(product_counts)
```

---

## 🎓 How to Demonstrate Your Project
To run the full pipeline, open **4 separate terminals** and run these commands in order:

1. **Terminal 1 (Start Kafka):**
   ```bash
   cd kafka_2.13-4.3.1
   bin/kafka-server-start.sh config/server.properties
   ```
2. **Terminal 2 (Start Producer):**
   ```bash
   source kafka_env/bin/activate
   python producer.py
   ```
3. **Terminal 3 (Start Spark Streaming):**
   ```bash
   source kafka_env/bin/activate
   python spark_processor.py
   ```
4. **Terminal 4 (Start Dashboard):**
   ```bash
   source kafka_env/bin/activate
   streamlit run dashboard.py
   ```
   *Streamlit will provide a localhost URL in the terminal (usually `http://localhost:8501`). Open this link in your browser to view the live updating graphs!*
