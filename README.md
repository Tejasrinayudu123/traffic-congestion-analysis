#  Cloud-Based Traffic Congestion Analysis using AWS & Apache Spark

## Project Overview
This project analyzes large-scale NYC taxi trip data to identify traffic patterns, detect congestion hotspots, and predict trip duration using machine learning.  
It is implemented as an end-to-end cloud pipeline using AWS services and Apache Spark.

---

##  Objectives
- Analyze taxi trip data at scale
- Identify peak traffic hours and high-demand zones
- Detect congestion hotspots
- Build machine learning models for trip duration prediction
- Design a scalable cloud-based pipeline

---

## ☁️ Cloud Architecture

S3 → EMR (Spark) → S3 → SageMaker

### 🔹 Workflow:
1. Raw data stored in Amazon S3
2. Processed using Apache Spark on EMR
3. Results stored back in S3
4. Visualized using SageMaker

---

## 🛠️ Technologies Used

- **Amazon S3** → Data storage
- **Amazon EMR** → Distributed processing
- **Amazon EC2** → Compute infrastructure (via EMR)
- **Apache Spark** → Data processing & ML
- **SageMaker** → Visualization
- **Python (PySpark)**

---

## 📂 Project Structure
traffic-congestion-analysis/
│
├── preprocessing.py
├── analysis.py
├── ml_model.py
│
├── README.md
│
└── screenshots/

---

## 📊 Dataset

- **Source:** NYC Taxi Dataset
- **Format:** Parquet + CSV
- **Size:** ~46 MB (scalable to GBs)

### 🔹 Features Used:
- Pickup & drop-off timestamps
- Pickup & drop-off location IDs
- Trip distance
- Passenger count

---

## ⚙️ Pipeline Phases

### 🔹 Phase 1: Setup
- Upload dataset to S3
- Create EMR cluster
- Configure Spark environment

### 🔹 Phase 2: Data Preprocessing
- Remove invalid and null values
- Compute trip duration
- Extract features:
  - pickup_hour
  - pickup_dayofweek
- Join zone lookup dataset

### 🔹 Phase 3: Data Analysis
- Trips by hour
- Top pickup zones
- Average trip duration
- Congestion hotspots

### 🔹 Phase 4: Machine Learning
- Linear Regression (baseline)
- Random Forest (final model)
- Feature vector creation
- Model evaluation

### 🔹 Phase 5: Visualization
- Load results from S3
- Generate graphs in SageMaker
- Export images

---

## 📈 Results

| Model | RMSE | MAE | R² |
|------|------|------|------|
| Linear Regression | 10.65 | 7.54 | 0.058 |
| Random Forest | 6.04 | 4.11 | 0.697 |

👉 Random Forest significantly outperformed Linear Regression.

---

## 🔥 Key Insights

- Peak traffic occurs during morning and evening hours
- Certain zones have consistently high demand
- Congestion hotspots show high trip duration and low speed
- Machine learning improves prediction accuracy

---

## ⚠️ Challenges Faced

- EMR cluster startup delay
- SageMaker not available in Learner Lab
- Large dataset handling limitations
- Spark job failures due to resource constraints
- Dependency issues (NumPy, libraries)
- S3 path and file management complexity

---

## 📚 Lessons Learned

- Importance of pipeline design
- Efficient use of cloud resources
- Feature engineering improves ML performance
- Debugging distributed systems is challenging

---

## 🚀 How to Run

### Step 1: Preprocessing
```bash
spark-submit preprocessing.py

### Step 2: Analysis
```bash
spark-submit analysis.py

### Step 3: Machine Learning
```bash
spark-submit ml_model.py


## 💡 Conclusion

This project demonstrates how cloud computing and distributed data processing can be used to solve real-world traffic analysis problems.
It highlights the effectiveness of Apache Spark and AWS services in handling large-scale datasets and generating meaningful insights.
