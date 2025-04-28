# Data Engineer Challenge: Healthcare Analytics Pipeline

## Overview

In this challenge, you will be asked to build a data pipeline that processes and transforms a sample dataset extracted from our EHR system. Your task is to design an ETL pipeline, clean and transform the data, and answer key business questions that would help healthcare professionals make better data-driven decisions.

## Setup

We've provided a Docker Compose setup with PostgreSQL to help you get started quickly. To use it:

1. Make sure you have Docker and Docker Compose installed
2. Run `docker-compose up -d` to start the PostgreSQL database
3. The database will be available at:
   - Host: localhost
   - Port: 5432
   - Database: healthcare
   - Username: postgres
   - Password: postgres

To help you get started quickly, we've provided some helper files:

1. `load_data.py`: A Python script to load the CSV files into PostgreSQL
   - Install dependencies: `pip install -r requirements.txt`
   - Run: `python load_data.py`

2. `sample_queries.sql`: Contains some basic SQL queries to help you explore the data
   - You can run these queries in your preferred SQL client
   - Feel free to modify and extend these queries for your analysis

If you prefer to use a different database solution, you're free to do so, but please provide clear instructions on how to set it up.

## Technical Requirements

We recommend using the following technologies:

- **Programming Language**: Python 3.8 or higher
- **Database**: SQL (PostgreSQL preferred)
- **Data Processing**: You can use any Python libraries you're comfortable with (e.g., pandas, SQLAlchemy)
- **Version Control**: Git

While these are our preferred technologies, you're free to use other tools if you believe they better solve the problem. However, please be prepared to explain your technology choices in your submission.

## Success Criteria

A successful submission will include:

1. **Documentation (README or PDF)**:
   - Clear explanation of your approach and design decisions
   - Step-by-step instructions to run your solution
   - Description of any assumptions made
   - Discussion of trade-offs and alternative approaches considered

2. **Data Pipeline Implementation**:
   - Working ETL pipeline that ingests and transforms the data
   - Clean, well-documented code
   - Proper error handling and data validation
   - Efficient data processing approach

3. **Analysis and Results**:
   - SQL queries used to answer the business questions
   - Results of the analysis in a clear, readable format
   - Visualizations or tables showing key metrics
   - Brief interpretation of the results and their business implications

4. **Code Quality**:
   - Well-structured, maintainable code
   - Clear documentation and comments
   - Proper error handling
   - Efficient data processing

### Example Output Format

Your documentation should include sections similar to this example:

```sql
-- Query: Average age of patients per appointment type
SELECT 
    a.appointment_type,
    AVG(p.age) as average_age
FROM appointments a
JOIN patients p ON a.patient_id = p.patient_id
GROUP BY a.appointment_type;

-- Results:
-- appointment_type | average_age
-- -----------------+-------------
-- Checkup         | 45.2
-- Emergency       | 38.7
-- ...

-- Analysis:
-- The data shows that patients scheduling checkups tend to be older...
```

## Dataset

The sample dataset contains the following tables:

1. **Patients**: Contains information about patients.
   - `patient_id` (integer)
   - `name` (string)
   - `age` (integer) - Note: Some demographic information may need careful handling
   - `gender` (string)
   - `registration_date` (date) - Important for understanding patient history

2. **Appointments**: Contains records of patient appointments.
   - `appointment_id` (integer)
   - `patient_id` (integer)
   - `appointment_date` (date) - Consider the relationship with patient registration dates
   - `appointment_type` (string: e.g., "Checkup", "Emergency", etc.)
   - `provider_id` (integer)

3. **Providers**: Information about healthcare providers.
   - `provider_id` (integer)
   - `name` (string)
   - `specialty` (string)

4. **Prescriptions**: Medication prescribed to patients.
   - `prescription_id` (integer)
   - `patient_id` (integer)
   - `medication_name` (string)
   - `prescription_date` (date) - Pay attention to prescription patterns

The CSV files are located in the `sample_datasets` directory. As with any real-world healthcare data, you may encounter some interesting patterns that require careful consideration during your analysis.

## Task

### Part 1: Data Pipeline

1. **Data Ingestion**:
   - Ingest the provided dataset into a database of your choice (or the one we provided on the docker-compose).
   - If you choose a different solution, please provide clear setup instructions.

2. **Data Transformation**:
   Create the following derived fields to enable better analysis:
   
   a) Patient-level transformations:
   - Age group (0-18, 19-30, 31-50, 51-70, 71+)
   - Patient type (New: registered < 6 months, Regular: 6-24 months, Long-term: > 24 months)
   
   b) Appointment-level transformations:
   - Day of week (Monday-Sunday)
   - Time since last appointment (in days)
   
   c) Prescription-level transformations:
   - Medication category (e.g., Pain Relief, Diabetes, Heart, etc.)
   - Prescription frequency (First-time, Repeat)

3. **Data Analysis**:
   Using the transformed data, answer the following questions:
   
   a) Patient Analysis:
   - What is the distribution of patients across age groups?
   - How does the appointment frequency vary by patient type?
   
   b) Appointment Analysis:
   - What are the most common appointment types by age group?
   - Are there specific days of the week with higher emergency visits?
   
   c) Prescription Analysis:
   - What are the most prescribed medication categories by age group?
   - How does prescription frequency correlate with appointment frequency?

### Part 2: Documentation

Create a comprehensive report that includes:

1. **Data Pipeline Documentation**:
   - Description of your data transformation approach
   - SQL queries or code used for transformations
   - Any data quality issues encountered and how you handled them

2. **Analysis Results**:
   - SQL queries used for each analysis question
   - Results presented in clear tables or visualizations
   - Brief interpretation of each finding

3. **Business Insights**:
   - 2-3 key findings that could help improve healthcare operations
   - Suggestions for further analysis

## Submission

Please submit your code and documentation in a GitHub repository or any version-controlled platform. Make sure to include:

1. All necessary code files
2. A README file explaining:
   - How to run your pipeline
   - Any assumptions you've made
   - Any trade-offs you've considered during the development process
   - Clear instructions for setting up and running your solution

If you used a different database solution than the provided PostgreSQL setup, please ensure that all data files and scripts are included and can be easily run on any local environment.

Alternatively, you may submit your solution as a zip folder via email if you prefer this method.

