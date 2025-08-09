# Calendly Call Bookings Analytics Dashboard

This project provides an end-to-end data pipeline and interactive dashboard to analyze call bookings from Calendly, combined with marketing spend data across multiple channels. The solution leverages AWS serverless and analytics services, with Streamlit for visualization and GitHub for CI/CD and workflow orchestration.

## Project Overview

The goal is to analyze Calendly booking events alongside marketing spend data from YouTube, Facebook, and TikTok. An interactive Streamlit dashboard queries the processed data to provide insights into call bookings in relation to marketing spend.

---

## Architecture & Workflow

<img width="4536" height="1424" alt="image" src="https://github.com/user-attachments/assets/24ef976a-06b6-451b-a74a-c81fba1807f9" />


### 1. Data Ingestion
- **Calendly webhook events** trigger an AWS Lambda function.
- The Lambda filters out marketing-related bookings and stores raw event files into an S3 bucket.
- A separate Lambda function extracts daily spend data from the company’s S3 bucket for three marketing channels (YouTube Ads, Facebook Ads, TikTok Ads) and places this data into the project’s S3 bucket.

### 2. Data Processing & Transformation
- An AWS Glue PySpark ETL job runs daily (or manually triggered via GitHub workflow).
- This job transforms and flattens the raw JSON booking data and writes it into a staging table in AWS Athena.
- The final curated tables are exposed as Athena views.
- A Glue crawler updates the daily spend table partitions in Athena based on new data arrivals.

  <img width="1310" height="842" alt="image" src="https://github.com/user-attachments/assets/ecf4e78a-fce0-4429-b191-059efe19c46c" />


### 3. Dashboarding
- A Streamlit app queries Athena to build an interactive dashboard, allowing users to explore booking trends and correlate them with marketing spend.

### 4. Deployment & Orchestration
- GitHub workflows manage CI/CD processes including:
  - Deploying Streamlit app updates.
  - Triggering Lambda functions.
  - Running Glue jobs.
  - Starting Glue crawlers.
- Workflows can be triggered automatically or manually via GitHub workflow dispatch.

---

## Technologies Used

- **AWS Lambda** – Serverless functions for event-driven data ingestion and processing.
- **AWS S3** – Storage for raw event and marketing spend data.
- **AWS Glue** – PySpark ETL jobs and crawlers for data transformation and cataloging.
- **AWS Athena** – Query engine for structured analytics on transformed data.
- **AWS API Gateway** – (If applicable) for managing API endpoints and integration.
- **Streamlit** – Interactive web app for data visualization and dashboarding.
- **GitHub Actions** – CI/CD pipelines and orchestration of AWS workflows.

---

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://calendlydeproject-appyqylkjarclbyan5flhr.streamlit.app/)

