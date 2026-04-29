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
## 🛠️ Technologies Used
- Amazon S3  
- Amazon EMR  
- Amazon EC2  
- Apache Spark (PySpark)  
- SageMaker  
- Python  

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

```bash
spark-submit preprocessing.py
spark-submit analysis.py
spark-submit ml_model.py
```

## 💡 Conclusion
This project successfully demonstrates how cloud-based big data technologies can be used to analyze and model real-world traffic patterns.

An end-to-end pipeline was built using AWS services, where data was stored in Amazon S3, processed using Apache Spark on EMR, and visualized using SageMaker. The system efficiently handled large-scale data and generated meaningful insights such as peak traffic hours, high-demand zones, and congestion hotspots.

In the machine learning phase, Random Forest significantly outperformed Linear Regression, achieving lower error values and a higher R² score. This highlights the importance of using advanced models for capturing complex traffic patterns.

Overall, this project showcases the power of scalable cloud infrastructure and distributed computing in solving real-world problems and can be extended further for real-time traffic monitoring and smart city applications.


