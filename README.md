# Real-Time eCommerce Dashboard with Apache Beam, GCP Pub/Sub, and Streamlit

This project demonstrates how to create a real-time dashboard for tracking eCommerce events such as purchases, add_to_cart, page views, and cart abandonment. It uses **Google Cloud Pub/Sub** for event streaming, **Apache Beam** on **Google Cloud Dataflow** for real-time data processing, and **Streamlit** for data visualization. Additionally, it integrates with **BigQuery** to display historical data.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Folder Structure](#folder-structure)
4. [Components](#components)
5. [How to Run](#how-to-run)
6. [Key Technologies](#key-technologies)

## Project Overview

This project captures real-time events from an eCommerce website (such as `purchase`, `add_to_cart`, `page_view`, `cart_abandonment`) and processes them using **Google Cloud Dataflow** with **Apache Beam**. The processed events are then displayed on a **Streamlit dashboard**, which consists of two parts:
1. **Real-time Dashboard**: Displays up-to-the-second updates on user actions.
2. **Historic Dashboard**: Displays aggregated insights from **BigQuery** for broader analysis over a longer period.

## Architecture

The architecture for this real-time dashboard is built around the following key components:

1. **Event Producer (Flask-based eCommerce Website)**: Sends user actions (like page views, cart interactions, and purchases) to **Google Cloud Pub/Sub**.
2. **Google Cloud Pub/Sub**: Acts as the message broker that streams events from the eCommerce website to the processing pipeline.
3. **Google Cloud Dataflow (Apache Beam)**: Processes real-time streams of data from Pub/Sub. The pipeline aggregates and transforms the data and sends it to two destinations:
   - **Streamlit**: For real-time visualization.
   - **BigQuery**: For historical data storage and further analysis.
4. **BigQuery**: Stores historical events for long-term analysis, accessible from the historic part of the dashboard.
5. **Streamlit**: Displays both real-time and historical data in a web interface for business insights.

![Architecture](architecture_diagram.png)  <!-- You can replace this with a real image if available -->

## Folder Structure


### Components

1. **cart_abandonment_pipeline.py**: Contains the Apache Beam pipeline to detect cart abandonment events based on user activity within a specific time window.
   
2. **event_transformer.py**: Handles data transformation from raw events into structured data that can be processed downstream. The transformation includes parsing metadata, handling events such as `add_to_cart`, `purchase`, and calculating total cart values.

3. **pubsub_io.py**: Manages Pub/Sub I/O streams, reading from the `ecommerce-events` Pub/Sub topic and writing to the `cart-abandonment` topic.

4. **enriched_user_events.py**: Enriches event data by adding metadata like session duration, product details, and user geolocation before writing the processed data to BigQuery.

5. **app.py**: The Streamlit web application for visualizing real-time and historical events. The real-time part gets data from Pub/Sub while the historical part fetches data from BigQuery.

### Streamlit Dashboard
- **Real-time Dashboard**: Displays ongoing user events, including purchase trends, cart abandonment rates, top products, and user sessions.
- **Historic Dashboard**: Aggregates historical data from BigQuery to provide insights into user behavior, revenue trends, conversion rates, and more.

## How to Run

### Prerequisites
- **Google Cloud Platform** account with Pub/Sub, Dataflow, and BigQuery enabled.
- **Apache Beam** (with Python SDK).
- **Streamlit** for the dashboard.
- **BigQuery** table for storing historical data.

