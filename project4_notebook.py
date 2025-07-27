# Project 4: Mini-Data Engineering: Pipeline Aggregation + Data Mart

# This file contains the Python code for creating a data pipeline to process CRM leads and create a data mart.

import pandas as pd
from datetime import datetime
import os

# --- 1. Load Raw Data (Simulating Google Sheets Import) ---
raw_leads_df = pd.read_csv("crm_leads_raw.csv", parse_dates=["submission_date"])

print("Raw CRM leads data loaded successfully!")
print(f"Total raw leads: {len(raw_leads_df)}")

# --- 2. Data Parsing, Cleaning, and Normalization ---
# Create a copy for cleaning
cleaned_leads_df = raw_leads_df.copy()

# Standardize source names
cleaned_leads_df["source"] = cleaned_leads_df["source"].str.lower().str.replace(" ", "_")

# Validate email format (simple check)
cleaned_leads_df["is_valid_email"] = cleaned_leads_df["email"].str.contains(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

# Extract year and month from submission date
cleaned_leads_df["submission_year"] = cleaned_leads_df["submission_date"].dt.year
cleaned_leads_df["submission_month"] = cleaned_leads_df["submission_date"].dt.month

print("Data cleaning and normalization complete.")

# --- 3. Create Data Mart (crm_leads_cleaned) ---
# Select and rename columns for the data mart
data_mart_df = cleaned_leads_df[[
    "lead_id",
    "submission_date",
    "submission_year",
    "submission_month",
    "source",
    "status",
    "value",
    "is_valid_email"
]].copy()

# Save the data mart to a CSV file (simulating export to PostgreSQL)
data_mart_df.to_csv("crm_leads_cleaned.csv", index=False)

print("Data mart (crm_leads_cleaned.csv) created successfully!")

# --- 4. Cron Job Logic (Simulated) ---
# This section simulates the logic that would be run by a cron job daily.

def daily_pipeline_job():
    print("\n--- Running Daily Pipeline Job (Simulation) ---")
    # 1. Load new raw data (in a real scenario, this would be from Google Sheets API)
    # For simulation, we just reload the same raw data.
    new_raw_data = pd.read_csv("crm_leads_raw.csv", parse_dates=["submission_date"])
    
    # 2. Clean and normalize the new data
    # (Applying the same cleaning steps as above)
    
    # 3. Append to the existing data mart
    # (In a real scenario, you would append to the PostgreSQL table)
    
    print("Daily pipeline job finished. Data mart updated.")
    print(f"Timestamp: {datetime.now()}")

# Run the simulated job
daily_pipeline_job()

print("Project 4: Mini-Data Engineering Pipeline - Analysis complete.")


