import pandas as pd
import numpy as np
import os
from datetime import datetime
import plotly.express as px
from fpdf import FPDF
import plotly.io as pio

# Initial configurations
current_path = os.getcwd()
print(f"Current path: {current_path}")
gold_path = os.path.join(current_path, "app", "bucket", "gold")
output_folder = os.path.join(current_path, "output")
os.makedirs(output_folder, exist_ok=True)

# Function to find the latest partition
def find_latest_partition(base_path):
    partitions = []
    for root, dirs, files in os.walk(base_path):
        if all(x in root for x in ["year=", "month=", "day="]):
            parts = root.split(os.sep)
            year = int([p.split('=')[1] for p in parts if p.startswith('year=')][0])
            month = int([p.split('=')[1] for p in parts if p.startswith('month=')][0])
            day = int([p.split('=')[1] for p in parts if p.startswith('day=')][0])
            partitions.append((year, month, day, root))
    if not partitions:
        raise FileNotFoundError(f"No partitions found in {base_path}")
    return partitions[-1][-1]

# Load data
latest_gold_partition = find_latest_partition(gold_path)
print(f"✅ Latest partition found: {latest_gold_partition}")

df_appointments = pd.read_parquet(os.path.join(latest_gold_partition, "appointments.parquet"))
df_patients = pd.read_parquet(os.path.join(latest_gold_partition, "patients.parquet"))
df_prescriptions = pd.read_parquet(os.path.join(latest_gold_partition, "prescriptions.parquet"))
df_providers = pd.read_parquet(os.path.join(latest_gold_partition, "providers.parquet"))
print("✅ Data successfully loaded from the latest gold partition!")

# Analysis
figures = []  # Store graph file paths
texts = []    # Store texts for the PDF

### A1 - Distribution of patients by age group
age_group_dist = df_patients['age_group'].value_counts().reset_index()
age_group_dist.columns = ['Age Group', 'Number of Patients']
texts.append("# A1 - Distribution of patients by age group")
texts.append(age_group_dist.to_string(index=False))

fig = px.bar(
    age_group_dist,
    x='Age Group',
    y='Number of Patients',
    title='Distribution of Patients by Age Group'
)
fig_path = os.path.join(output_folder, "A1_age_distribution.png")
fig.write_image(fig_path)
figures.append(fig_path)

### A2 - Appointment frequency by patient type
df1 = df_appointments.merge(df_patients, on='patient_id', how='left')
df1 = df1[df1['days_since_last_appointment'] >= 0]

analysis = df1.groupby('patient_type')['days_since_last_appointment'].agg(['count', 'mean', 'median', 'std']).round(2)
texts.append("# A2 - Appointment frequency by patient type")
texts.append(analysis.to_string())

### B1 - Most common appointment types by age group
appointment_counts = df1.groupby(['age_group', 'appointment_type']).size().reset_index(name='count')

fig = px.bar(
    appointment_counts,
    x='age_group',
    y='count',
    color='appointment_type',
    barmode='group',
    title='Most Common Appointment Types by Age Group',
    labels={'age_group': 'Age Group', 'count': 'Number of Appointments', 'appointment_type': 'Appointment Type'}
)
fig_path = os.path.join(output_folder, "B1_appointment_types.png")
fig.write_image(fig_path)
figures.append(fig_path)

top_appointment_counts = (
    appointment_counts.sort_values(["age_group", "count"], ascending=[True, False])
    .drop_duplicates(subset=["age_group"])
    .reset_index(drop=True)
)
texts.append("# B1 - Most common appointment type by age group")
texts.append(top_appointment_counts.to_string(index=False))

### B2 - Days of the week with the most emergencies
emergency_days = (
    df_appointments[df_appointments['appointment_type'] == 'Emergency']
    .groupby('day_of_week')["appointment_id"].count()
    .reset_index(name='count')
    .sort_values(by='count', ascending=False)
)
texts.append("# B2 - Days of the week with the most emergency visits")
texts.append(emergency_days.to_string(index=False))

fig = px.bar(
    emergency_days,
    x='day_of_week',
    y='count',
    title='Number of Emergency Visits by Day of the Week',
    labels={'day_of_week': 'Day of the Week', 'count': 'Number of Emergencies'}
)
fig_path = os.path.join(output_folder, "B2_emergencies_per_day.png")
fig.write_image(fig_path)
figures.append(fig_path)

### C1 - Most prescribed medication categories by age group
df2 = df_prescriptions.merge(df_patients, on='patient_id', how='left')

medication_group = df2.groupby(["age_group", "medication_category"]).size().reset_index(name="count")
most_common_medications = (
    medication_group.sort_values(["age_group", "count"], ascending=[True, False])
    .drop_duplicates(subset=["age_group"])
    .reset_index(drop=True)
)
texts.append("# C1 - Most prescribed medication categories by age group")
texts.append(most_common_medications.to_string(index=False))

fig = px.bar(
    medication_group,
    x="age_group",
    y="count",
    color="medication_category",
    barmode="group",
    title="Most Prescribed Medication Categories by Age Group"
)
fig_path = os.path.join(output_folder, "C1_most_prescribed_medications.png")
fig.write_image(fig_path)
figures.append(fig_path)

### C2 - Correlation between appointment and prescription frequency
patient_appointments = df1.groupby('patient_id').size().reset_index(name='num_appointments')
patient_prescriptions = df_prescriptions.groupby('patient_id').size().reset_index(name='num_prescriptions')

patient_activity = patient_appointments.merge(patient_prescriptions, on='patient_id', how='inner')
correlation = patient_activity['num_appointments'].corr(patient_activity['num_prescriptions'])

texts.append("# C2 - Correlation between appointment and prescription counts")
texts.append(f"Pearson correlation: {correlation:.2f}")

fig = px.scatter(
    patient_activity,
    x='num_appointments',
    y='num_prescriptions',
    trendline='ols',
    title='Correlation between Appointment and Prescription Counts',
    labels={'num_appointments': 'Number of Appointments', 'num_prescriptions': 'Number of Prescriptions'}
)
fig_path = os.path.join(output_folder, "C2_correlation.png")
fig.write_image(fig_path)
figures.append(fig_path)

# Create PDF report
class PDF(FPDF):
    def chapter_title(self, title):
        self.set_font('Arial', 'B', 14)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 11)
        self.multi_cell(0, 8, body)
        self.ln()

    def insert_image(self, image_path):
        self.image(image_path, w=180)
        self.ln(10)

pdf = PDF()
pdf.set_auto_page_break(auto=True, margin=15)
pdf.add_page()

for i, text in enumerate(texts):
    if text.startswith("#"):
        pdf.chapter_title(text.replace("#", "").strip())
    else:
        pdf.chapter_body(text)
        if figures:
            pdf.insert_image(figures.pop(0))

pdf_output_path = os.path.join(current_path, "Report.pdf")
pdf.output(pdf_output_path)

print(f"✅ Report saved at: {pdf_output_path}")
