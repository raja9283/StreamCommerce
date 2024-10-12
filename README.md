# Real-Time eCommerce Dashboard with Apache Beam, GCP Pub/Sub, and Streamlit

This project demonstrates how to create a real-time dashboard for tracking eCommerce events such as purchases, add_to_cart, page views, and cart abandonment. It uses **Google Cloud Pub/Sub** for event streaming, **Apache Beam** on **Google Cloud Dataflow** for real-time data processing, and **Streamlit** for data visualization. Additionally, it integrates with **BigQuery** to display historical data.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Key Technologies](#key-technologies) 
4. [Components](#components)
5. [How to Run](#how-to-run)
6. [Streamlit Dashboard](#streamlit-dashboard)

## Project Overview
This project implements a real-time dashboard for an eCommerce application, designed to monitor and analyze user interactions and sales performance. By leveraging Google Cloud's suite of services, the architecture efficiently processes clickstream data, providing valuable insights to business stakeholders.

The project captures real-time events from the eCommerce website, such as purchase, add_to_cart, page_view, and cart_abandonment, processing them using **Google Cloud Dataflow** with **Apache Beam**. The processed events are then displayed on a **Streamlit** dashboard, which consists of two parts:
1. **Real-time Dashboard**: Displays up-to-the-second updates on user actions.
2. **Historic Dashboard**: Displays aggregated insights from **BigQuery** for broader analysis over a longer period.

## Architecture

The architecture for this real-time dashboard is built around the following key components:

1. **Event Producer (Compute Engine + Cloud SQL + Python Script)**: Instead of using a Flask-based web application, the event producer is implemented using a **Compute Engine** instance paired with **Cloud SQL**. A Python script processes clickstream data and streams this data in real-time to **Google Cloud Pub/Sub**.
2. **Google Cloud Pub/Sub**: Acts as the message broker that streams events from the eCommerce website to the processing pipeline.
3. **Google Cloud Dataflow (Apache Beam)**: Processes real-time streams of data from Pub/Sub. The pipeline aggregates and transforms the data and sends it to two destinations:
   - **Streamlit**: For real-time visualization.
   - **BigQuery**: For historical data storage and further analysis.
4. **BigQuery**: Stores historical events for long-term analysis, accessible from the historic part of the dashboard.
5. **Streamlit**: Displays both real-time and historical data in a web interface for business insights.
> Sample data that is loaded in Cloud SQL for real-time event creation can be found [here](https://www.kaggle.com/datasets/king9283/ecommerce-realtime-analysis).

![ecommerce_analysis_diagram](https://github.com/user-attachments/assets/50523708-f43b-4805-8271-25ca7342782f)

## Key Technologies
- **Google Cloud Pub/Sub**: A messaging service for real-time event streaming that allows asynchronous communication between services, ensuring reliable message delivery at scale and with built-in durability.
- **Google Cloud Dataflow**: A fully managed service powered by Apache Beam for processing and transforming both streaming and batch data, enabling real-time analytics with auto-scaling capabilities and efficient data pipeline management.
- **BigQuery**: A serverless data warehouse designed for large-scale data analytics, allowing users to execute fast SQL queries on massive datasets and seamlessly integrate with other Google Cloud services.
- **Streamlit**: An open-source app framework for creating interactive web applications, enabling developers to build real-time dashboards that visualize data easily and share insights with minimal coding effort.
- **Apache Beam**: An open-source unified model for defining data processing workflows, allowing for event data transformation and processing across various execution engines like Google Cloud Dataflow, enhancing flexibility and scalability.

## Components

1. **cart_abandonment.py**: This Apache Beam pipeline identifies abandoned shopping carts in an eCommerce application using Google Cloud Pub/Sub for real-time data processing. The pipeline processes events like ADD_TO_CART and BOOKING, then detects cart abandonment when users add items to their cart but don't complete a purchase.

2. **event_metrics.py**: This Apache Beam pipeline is designed to capture and calculate real-time metrics for key eCommerce events (such as page_view, add_to_cart, and purchase) from a Google Cloud Pub/Sub stream. The pipeline computes aggregate metrics over sliding time windows and publishes the results back to Pub/Sub.

3. **trending_info.py**: This pipeline processes real-time sales event data (like BOOKING and ADD_TO_CART) from a Pub/Sub stream to identify the top 10 products by sales quantity and total revenue within a sliding window of 1 hour

4. **enriched_user_events.py**: This script sets up a real-time data processing pipeline using Apache Beam to ingest, enrich, transform, and store event data. The pipeline processes events streamed from a Google Cloud Pub/Sub subscription, enriches them with customer and product data retrieved from a PostgreSQL database, and writes the enriched data to a BigQuery table for further analysis.

5. **dashboard.py**: This Streamlit application serves as a real-time dashboard for monitoring key eCommerce events, providing insights into user interactions and sales performance. It captures data from Google Cloud Pub/Sub, displaying real-time metrics related to user activity on the website. The application is designed to help users:

   - **Real-time Event Monitoring**: Users can view live updates on critical eCommerce events such as add_to_cart, page_view, purchase, and cart abandonment. This enables businesses to react promptly to user behavior and trends.

   - **Data Visualization**: The dashboard presents various visualizations, including line charts and other graphical representations of metrics like sales trends, abandoned carts, and total lost sales. This makes it easier for users to analyze data and identify patterns at a glance.

   - **Historical Data Insights**: In addition to real-time data, the application allows users to access historical data stored in BigQuery. This feature helps in performing trend analysis and understanding long-term user behavior and sales performance.

   - **User-Friendly Interface**: The application is designed with a simple and intuitive interface, making it accessible for users without technical expertise to navigate and interpret the data effectively.

## How to Run

### Prerequisites
- **Google Cloud Platform** account with Pub/Sub, Dataflow, and BigQuery enabled.
- **Apache Beam** (with Python SDK).
- **Streamlit** for the dashboard.
### Step 1: Set up Pub/Sub topics
 - refer sample.env and create all the topics and subscriptions mentioned in it.
### Step 2: Run Apache Beam Pipeline
 - There are 2 ways to run this pipeline.
      1. Local :  You can run this pipeline locally after installing the required packages. You can execute this as you would any other Python file; just make sure that the runner is set to DirectRunner.
      2. GCP Dataflow: Go to Cloud Shell, add your Python code there, and change the runner to DataflowRunner. Then, use the command below to initiate your Dataflow pipeline
            ```bash
         python trending_info.py   --runner DataflowRunner   --project [PROJECT_ID] --staging_location [STAGING_BKT]   --temp_location [TEMP_BKT] --region [REGION]

### Step 3: Run Streamlit
- Run the Streamlit app locally:
  ```bash
  streamlit run streamlit/dashboard.py

### Step 4: View the dashboard
- Open the Streamlit app in your browser (http://localhost:8501) to see real-time updates and historical data insights.

## Streamlit Dashboard
- **Real-time Dashboard**: Displays ongoing user events, including purchase trends, cart abandonment rates, top products, and user sessions.
  
  ![realtime dashboard](https://github.com/user-attachments/assets/ef1551b7-b001-4a41-a6f9-bf002759dae1)

- **Historic Dashboard**: Aggregates historical data from BigQuery to provide insights into user behavior, revenue trends, conversion rates, and more.
  
  ![historic dashboard](https://github.com/user-attachments/assets/f62e48e3-4fac-46b9-a94d-a62ebb20e999)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
