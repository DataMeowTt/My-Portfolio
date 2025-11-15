# HealthClaim Standardizer: Multi-Source Healthcare Data Standardization Pipeline

## 🎯 Project Objective

The **HealthClaim Standardizer** project is designed to address a major challenge in Healthcare Data Engineering: **standardizing raw data from multiple customers** (Multi-Customer Data Ingestion) into a single data warehouse.

The core goal is to create a unified raw layer (Standard Schema) in the Data Warehouse, allowing analytics teams to run queries and analytical models across the entire dataset without worrying about the original source format.

<img width="647" height="342" alt="hide" src="https://github.com/user-attachments/assets/1edbf7a4-dc02-4594-a2b6-abc3a3248f77" />

---

## Datasets

The project uses two main data sources, simulating a real-world healthcare data environment:

1. **CMS Claims Data:**
    * **Source:** Synthetic Medicare Claims and Enrollment Data.
    * **Purpose:** Provides claims and beneficiary information with complex ICD and HCPCS codes, simulating the structure of the CMS Research Identifiable File (RIF).
    * **Link:** [CMS Synthetic Data Collection](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event)

2. **NDC Drug Code Data (National Drug Code):**
    * **Source:** NDC file from OpenFDA (contains information about all FDA-approved drugs).
    * **Link:** [OpenFDA NDC Drug Data](https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip)

<img width="600" height="332" alt="data lúc 00 09 08" src="https://github.com/user-attachments/assets/594d209b-7b6a-4374-a224-89851278fc48" />

---

## Architecture & Pipeline (ELT Pipeline)

The system is built on **Apache Airflow** (orchestration), **Snowflake** (Data Warehouse), and **PostgreSQL** (Metastore/Mapping).

### 1. Dynamic Standardization Strategy

To handle multiple source schemas, the pipeline uses a dynamic mapping mechanism, enabling easy integration of new customer data without modifying the Airflow framework:

* **Logic:** When a CSV file enters the system, the Airflow task **uses the file name (`file_name`)** to look up the corresponding entry in the **`customer_sql_mapping`** table (in PostgreSQL).
* **Outcome:** The system automatically identifies and loads the corresponding **transformation SQL script** (`transform_customer_X.sql`).
* **Standardization:** This SQL script is then executed in Snowflake, ensuring that the raw data from that customer is transformed and loaded into a **single unified target schema** (e.g., `HEALTHCARE_DWH.RAW`).

### 2. Data Enrichment

The **`fda_download`** DAG ensures that the NDC drug code data is updated monthly and stored locally. This NDC data is crucial for advanced analytics, as it allows:

* **NDC Translation:** Converting drug codes (NDC) into brand names, active ingredients, and dosage information.
* **Advanced Analytics:** Incorporating detailed drug information into the DWH to support pharmaceutical cost analysis, prescribing trends, and drug utilization studies.

### 3. Unified Raw Data Layer

All source data is stored in a single schema (`HEALTHCARE_DWH.RAW`) with tables following a standardized schema. This creates a clean, reliable foundation for building downstream analytical layers (STANDARD, ANALYTICS).

---

## Project Ideas
Here’s a more specific list of potential projects based on individual diagnoses, treatments, and claim patterns. These include opioid-related projects as well as other diagnosis-specific ideas.

---

### **Opioid-Related Projects**

1. **Opioid Overprescription Detection**
   - **Objective**: Identify providers who prescribe opioids more frequently than peers for similar diagnoses.
   - **Approach**:
     - Compare opioid-related HCPCS codes across providers for diagnoses like back pain or minor injuries.
     - Include geographic and demographic factors for context.

2. **Inappropriate Opioid Prescriptions**
   - **Objective**: Detect instances where opioids are prescribed for diagnoses not typically requiring them.
   - **Approach**:
     - Cross-reference opioid prescriptions with ICD codes (e.g., prescribing opioids for sprains or headaches).
     - Highlight outliers for further investigation.

3. **Tracking Opioid Treatment Pathways**
   - **Objective**: Understand how often opioid prescriptions escalate into long-term treatments.
   - **Approach**:
     - Track patients over time to study transitions from short-term prescriptions to chronic opioid use.
     - Analyze claim lines and HCPCS codes for recurring opioid-related visits.

---

### **Diagnosis-Specific Projects**

6. **Overuse of Imaging**
   - **Objective**: Detect excessive imaging for minor injuries.
   - **Approach**:
     - Look for diagnoses like sprains and strains paired with repeated imaging claims (e.g., 73610).
     - Highlight providers with unusually high imaging rates.

7. **Inappropriate Emergency Room Visits**
   - **Objective**: Identify claims for non-emergency conditions in ER settings.
   - **Approach**:
     - Cross-reference diagnoses and HCPCS codes with provider types and locations.
     - Highlight cases like minor injuries treated in ERs instead of urgent care.

---

### **Chronic Conditions**
8. **Diabetes Management Patterns**
   - **Objective**: Study trends in the treatment of diabetes across claims.
   - **Approach**:
     - Analyze procedures and medications associated with diabetes diagnoses.
     - Detect gaps in care, such as missing standard treatments like HbA1c tests.

9. **Heart Disease Treatment Pathways**
   - **Objective**: Analyze common pathways for heart disease patients.
   - **Approach**:
     - Study claims with heart disease diagnoses and track treatments like stents or bypass surgeries.
     - Build a model to predict the next step in a patient’s treatment.

---

### **Provider-Level Insights**
12. **Surgical Complication Rates**
    - **Objective**: Measure rates of complications for surgeries.
    - **Approach**:
      - Analyze claims with postoperative diagnoses or additional treatments for infections, bleeding, or other complications.

13. **Chronic Pain Treatment Analysis**
    - **Objective**: Study providers treating chronic pain for potential overuse of certain treatments (e.g., opioids, imaging).
    - **Approach**:
      - Correlate pain-related diagnoses with the frequency of invasive procedures or medications.

14. **Provider Specialization Consistency**
    - **Objective**: Detect providers treating conditions outside their specialty.
    - **Approach**:
      - Analyze claims for patterns where diagnoses and treatments do not align with the provider’s primary focus.

---

### **Population-Level Patterns**
15. **Pediatric Treatment Trends**
    - **Objective**: Study trends in claims for pediatric diagnoses and treatments.
    - **Approach**:
      - Focus on common pediatric issues like fractures, infections, or asthma.
      - Compare provider and treatment variability.

16. **Elderly Fall Prevention**
    - **Objective**: Identify predictors for falls among the elderly.
    - **Approach**:
      - Study claims with diagnoses like fractures or external causes related to falls (e.g., W18).
      - Suggest interventions based on patterns.

---

### **Cost and Efficiency**
17. **Duplicate Claim Detection**
    - **Objective**: Identify claims with similar diagnoses and procedures submitted multiple times.
    - **Approach**:
      - Look for matching ICD and HCPCS codes within a short time frame for the same patient.

18. **Procedure Cost Benchmarking**
    - **Objective**: Compare costs for similar procedures across providers and regions.
    - **Approach**:
      - Use claims data to benchmark average costs for common treatments.

19. **Underutilized Preventive Care**
    - **Objective**: Detect cases where preventive treatments (e.g., vaccines, screenings) are underutilized.
    - **Approach**:
      - Cross-reference claims with recommended guidelines for certain populations.

---

## Important Claims Concepts

It's tempting to dive straight into the data, but its important to understand what the data represents. Otherwise, you're not going to understand why there are multiple ICD codes, and what they represent. You might not understand what inpatient vs outpatient mean and why they are different.

So let's dive into some of the basics around claims data.

---

### **1. ICD Codes (International Classification of Diseases)**

**Purpose**:  
ICD codes are standardized codes used globally to classify and code diseases, symptoms, and procedures. They are critical for documenting diagnoses and conditions. But, what might get confusing is there are both procedure and diagnosis codes.

- **ICD Diagnosis Codes**: 
  - Represent the medical reason for a patient's visit or condition (e.g., diabetes, hypertension).
  - Format: **ICD-10-CM** (e.g., E11.9 for Type 2 Diabetes Mellitus without complications).

- **ICD Procedure Codes**:  
  - Represent procedures performed during inpatient hospital stays and is used for reimbursement, quality reporting, and identifying inpatient care trends.
  - Format: **ICD-10-PCS** (e.g., 0U5B7ZZ for an open hysterectomy).
---

### **2. CPT Codes (Current Procedural Terminology)**

**Purpose**:  
CPT codes describe the specific medical, surgical, and diagnostic services provided by healthcare professionals.

- **Format**: Five-digit numeric codes (e.g., 99213 for a standard outpatient office visit). Some codes also include a modifier for additional specificity.
- **Categories**:
  - **Category I**: Common procedures and services.
  - **Category II**: Performance measurement (optional reporting codes).
  - **Category III**: Emerging technologies and experimental procedures.

---

### **3. HCPCS Codes (Healthcare Common Procedure Coding System)**

**Purpose**:  
HCPCS codes expand on CPT codes to include non-physician services like durable medical equipment (DME), transportation, or drugs.

- **Level I**: Equivalent to CPT codes.
- **Level II**: Covers services and equipment not included in CPT (e.g., E0601 for a CPAP machine).

---

### **4. DRG Codes (Diagnosis-Related Groups)**

**Purpose**:  
DRG codes classify inpatient stays into groups based on diagnoses and procedures. These are used primarily for hospital reimbursement.

- Example: DRG 470 for a major joint replacement or reattachment procedure.

---

### **5. Revenue Codes**

**Purpose**:  
Revenue codes indicate the department or type of service provided (e.g., emergency room, pharmacy). They are essential for understanding claims at the departmental level.

- Example: Revenue code 450 for emergency room services.

---

### **6. NDC (National Drug Codes)**

**Purpose**:  
NDC codes are unique identifiers for drugs and are used in claims involving prescriptions or administered medications.

- Format: 10-11 digit numeric code (e.g., 0781-1506-10 for a specific vial of insulin).

---

### **Practical Use Cases for Analysts**

1. **Cost Analysis**: Identify the highest-cost services and procedures using CPT or HCPCS codes.
2. **Population Health**: Segment populations by diagnosis (ICD codes) to identify trends or disparities in care.
3. **Utilization Analysis**: Determine service utilization rates by department (revenue codes) or service type.
4. **Performance Metrics**: Evaluate healthcare provider performance based on DRG benchmarks or CPT-related outcomes.
5. **Fraud Detection**: Identify anomalies in claims data, such as duplicate billing or mismatched codes.

By mastering these concepts, data analysts can extract actionable insights from claims data, helping improve operational efficiency, financial performance, and patient outcomes.
