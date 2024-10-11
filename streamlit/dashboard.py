import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
import plotly.express as px
from google.cloud import pubsub_v1
import json
import queue
import time
import os
from dotenv import load_dotenv

# Get the current working directory and move one folder up
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path)

project_id = os.getenv('project_id')
funnel_subscription = os.getenv('funnel_subscription')
kpi_subscription = os.getenv('kpi_subscription')
cart_abandonment_subscription = os.getenv('cart_abandonment_subscription')
bigquery_tbl = f"{os.getenv('dataset_id')}.{os.getenv('table_id')}"

# Set up Streamlit configurations
st.set_page_config(page_title="eCommerce Analysis", page_icon="🛒", layout="wide")

# Create queues to safely pass messages between threads
funnel_message_queue = queue.Queue()
kpi_message_queue = queue.Queue()
cart_abandonment_queue = queue.Queue()

# Initialize Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()


# Subscription paths
funnel_subscription_path = subscriber.subscription_path(project_id, funnel_subscription)
kpi_subscription_path = subscriber.subscription_path(project_id, kpi_subscription)
cart_abandonment_subscription_path = subscriber.subscription_path(project_id, cart_abandonment_subscription)

# Global funnel data dictionary
funnel_data = {
    "session_count": 0,
    "add_to_cart_count": 0,
    "purchase_count": 0,
}

abandonment_trend = {
                        "abandoned_count" : [],
                        "total_lost_sales" : [],
                        "timestamp" : []
                    }
# Callback functions to process incoming messages
def funnel_callback(message):
    global funnel_data
    try:
        message_data = json.loads(message.data.decode('utf-8'))
        for key, value in message_data.items():
            if key in funnel_data:
                funnel_data[key] = value
        funnel_message_queue.put(funnel_data)
        message.ack()
    except Exception as e:
        print(f"Error processing funnel message: {e}")
        message.nack()

def kpi_callback(message):
    try:
        message_data = json.loads(message.data.decode("utf-8"))
        kpi_message_queue.put(message_data)
        message.ack()
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        message.nack()

def cart_abandonment_callback(message):
    try:
        event_data = json.loads(message.data.decode("utf-8"))
        cart_abandonment_queue.put(event_data)
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

# Subscribe to Pub/Sub topics
funnel_future = subscriber.subscribe(funnel_subscription_path, callback=funnel_callback)
kpi_future = subscriber.subscribe(kpi_subscription_path, callback=kpi_callback)
cart_abandonment_future = subscriber.subscribe(cart_abandonment_subscription_path, callback=cart_abandonment_callback)

# Helper function to create a funnel chart
def create_funnel_chart(data):
    stages = ["Sessions", "Added to Cart", "Purchased"]
    count_values = [data['session_count'], data['add_to_cart_count'], data['purchase_count']]
    fig_funnel = go.Figure(go.Funnel(
        y=stages,
        x=count_values,
        textinfo="value+percent initial",
        marker=dict(color=['#FFA07A', '#6495ED', '#FFD700'])
    ))
    fig_funnel.update_layout(
        title='User Conversion Funnel',
        margin=dict(l=100, r=100, t=50, b=50),
    )
    return fig_funnel

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page", ["Realtime Dashboard", "Historic Dashboard"])

# Create separate placeholders for each page's content
dashboard_placeholder = st.empty()
insights_placeholder = st.empty()

# Define function for Dashboard page
def render_dashboard():
    with dashboard_placeholder.container():
        st.title("eCommerce Realtime Dashboard")
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1_placeholder = col1.empty()
        col2_placeholder = col2.empty()
        col3_placeholder = col3.empty()
        col4_placeholder = col4.empty()
        col5_placeholder = col5.empty()
        col6_placeholder = col6.empty()
        st.header("Top 10 Product(Add To Cart)")
        add_to_cart_table_placeholder = st.empty()
        st.header("Top 10 Product(Purchase)")
        booking_table_placeholder = st.empty()
        funnel_placeholder = st.empty()
        line_chart_placeholder = st.empty()


        while True:
            try:
                # Handle KPI updates
                if not kpi_message_queue.empty():
                    kpi_data = kpi_message_queue.get()
                    add_to_cart_data = kpi_data.get("ADD_TO_CART", {})
                    booking_data = kpi_data.get("BOOKING", {})

                    add_to_cart_total_quantity = int(add_to_cart_data.get("total_quantity", 0))
                    booking_total_quantity = int(booking_data.get("total_quantity", 0))
                    add_to_cart_total_sales = float(add_to_cart_data.get("total_amount", 0.0))
                    booking_total_sales = float(booking_data.get("total_amount", 0.0))

                    col1_placeholder.metric("Total Quantity (Add to Cart)", add_to_cart_total_quantity)
                    col2_placeholder.metric("Total Quantity (Booking)", booking_total_quantity)
                    col3_placeholder.metric("Total Sales (Add to Cart)", f"${add_to_cart_total_sales:.2f}")
                    col4_placeholder.metric("Total Sales (Booking)", f"${booking_total_sales:.2f}")

                    add_to_cart_top10 = pd.DataFrame(add_to_cart_data.get("top10", []))
                    booking_top10 = pd.DataFrame(booking_data.get("top10", []))

                    if not add_to_cart_top10.empty:
                        add_to_cart_top10.columns = ["Product ID", "Total Quantity", "Total Sales ($)"]
                        add_to_cart_table_placeholder.subheader("Top 10 Products (Add to Cart)")
                        add_to_cart_table_placeholder.table(add_to_cart_top10)

                    if not booking_top10.empty:
                        booking_top10.columns = ["Product ID", "Total Quantity", "Total Sales ($)"]
                        booking_table_placeholder.subheader("Top 10 Products (Booking)")
                        booking_table_placeholder.table(booking_top10)

                # Handle cart abandonment updates
                if not cart_abandonment_queue.empty():
                    cart_event_data = cart_abandonment_queue.get()
                    # cart_abandonment_data.append(cart_event_data)
                    
                    abandonment_trend['abandoned_count'].append(cart_event_data['abandoned_count'])
                    abandonment_trend['total_lost_sales'].append(cart_event_data['total_lost_sales'])
                    abandonment_trend['timestamp'].append(cart_event_data['timestamp'])
                    col5_placeholder.metric("Abandoning Carts", cart_event_data.get("abandoned_count", 0))
                    col6_placeholder.metric("Total Lost Sales", f"${cart_event_data.get('total_lost_sales', 0.0):.2f}")


                    fig = px.line(abandonment_trend, x="timestamp", y=["abandoned_count", "total_lost_sales"],
                                labels={"value": "Count / Sales ($)", "Time": "Time"},
                                title="Cart Abandonment Trend",
                                template="plotly_dark")
                    fig.update_traces(mode="lines+markers")
                    line_chart_placeholder.plotly_chart(fig, use_container_width=True)

                # Handle funnel updates
                if not funnel_message_queue.empty():
                    funnel_data = funnel_message_queue.get()
                    funnel_fig = create_funnel_chart(funnel_data)
                    funnel_placeholder.plotly_chart(funnel_fig, use_container_width=True)

                time.sleep(1)  # Process data in batches every 1 second for smooth UI updates

            except Exception as e:
                print(f"Error in dashboard update: {e}")


# Define function for Insights page
def render_insights():
    with insights_placeholder.container():
        # Set up Streamlit components
        st.title("eCommerce Historic Dashboard")
        # Initialize BigQuery Client
        client = bigquery.Client()

        # Use Streamlit columns to place start date, end date, and event selection on the same line
        col1, col2 = st.columns(2)

        # User selects the date range
        with col1:
            start_date = st.date_input(
                "Start Date", value=pd.to_datetime("today") - pd.Timedelta(days=30)
            )

        with col2:
            end_date = st.date_input("End Date", value=pd.to_datetime("today"))


        # Convert to string format for BigQuery
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Define queries for KPIs
        kpi_queries = {
            "Total eComm Purchases": """
                SELECT COUNT(*) AS total_purchases
                FROM {bigquery_tbl}
                WHERE event_name = 'BOOKING'
                AND DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}';
            """,
            "Conversion Rate": """
                SELECT 
                    COUNT(DISTINCT CASE WHEN event_name = 'BOOKING' THEN customer_id END) * 100.0 / COUNT(DISTINCT session_id) AS conversion_rate
                FROM {bigquery_tbl}
                WHERE DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}';
            """,
            "Purchase Revenue": """
                SELECT SUM(price_usd * quantity) AS total_revenue
                FROM {bigquery_tbl}
                WHERE event_name = 'BOOKING'
                AND DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}';
            """,
            "Average Order Value": """
                SELECT AVG(price_usd * quantity) AS average_order_value
                FROM {bigquery_tbl}
                WHERE event_name = 'BOOKING'
                AND DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}';
            """,
            "Revenue per User": """
                SELECT 
                    SUM(price_usd * quantity) / COUNT(DISTINCT customer_id) AS revenue_per_user
                FROM {bigquery_tbl}
                WHERE event_name = 'BOOKING'
                AND DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}';
            """
        }

        # Store results
        kpi_results = {}

        for kpi_name, query in kpi_queries.items():
            formatted_query = query.format(start_date_str=start_date_str, end_date_str=end_date_str,bigquery_tbl=bigquery_tbl)
            kpi_results[kpi_name] = client.query(formatted_query).to_dataframe().iloc[0, 0]


        # Create columns for each KPI
        columns = st.columns(5)  # Adjust the number of columns as needed
        
        # Display KPIs in respective columns
        for i, (kpi_name, value) in enumerate(kpi_results.items()):
            col = columns[i % 5]  # Choose the column based on index
            if "Rate" in kpi_name:  # For conversion rate
                col.metric(label=kpi_name, value=f"{value:.2f}%")  # No delta for rate
            else:
                col.metric(label=kpi_name, value=f"${value:,.2f}")  # Format as currency


        # Define queries for pie charts
        device_type_query = f"""
            SELECT device_type,traffic_source, round(SUM(price_usd * quantity),2) AS total_sales
            FROM {bigquery_tbl}
            WHERE event_name = 'BOOKING'
            GROUP BY device_type,traffic_source;
        """

        gender_query = f"""
            SELECT gender, SUM(price_usd * quantity) AS total_sales
            FROM {bigquery_tbl}
            WHERE event_name = 'BOOKING'
            GROUP BY gender;
        """


        col1, col2 = st.columns(2)

        # Pie chart for device type sales
        with col1:
            st.subheader('Sales by Device Type and Traffic Source')
            device_type_sales = client.query(device_type_query).to_dataframe()
            device_pie = px.sunburst(device_type_sales, path=['device_type', 'traffic_source'], values='total_sales')
            st.plotly_chart(device_pie)

        # Pie chart for gender sales
        with col2:
            st.subheader('Sales by Gender')
            gender_sales = client.query(gender_query).to_dataframe()
            gender_pie = px.pie(gender_sales, names='gender', values='total_sales')
            st.plotly_chart(gender_pie)


        # Define the BigQuery SQL query with dynamic date and event name filtering
        query = f"""
        WITH date_filtered AS (
        SELECT 
            DATE(event_time) AS event_date, 
            event_name, 
            COUNT(*) AS count
        FROM {bigquery_tbl}
        WHERE DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}'
            AND event_name IN ('ADD_TO_CART','BOOKING')
        GROUP BY event_date, event_name
        )

        SELECT 
        event_date,
        event_name,
        SUM(count) AS total_users
        FROM date_filtered
        GROUP BY event_date, event_name
        ORDER BY event_date;
        """

        # Execute the query and get the results in a DataFrame
        df = client.query(query).to_dataframe()

        # Create an interactive line chart using Plotly
        fig = px.line(
            df,
            x="event_date",
            y="total_users",
            color="event_name",
            labels={
                "event_date": "Date",
                "total_users": "Event Count",
                "event_name": "Event Name",
            },
            markers=True,
        )

        # Customize the chart with interactivity
        fig.update_layout(
            hovermode="x unified",
            xaxis_title="Date",
            yaxis_title="Count",
            legend_title="Event Type",
        )
        st.subheader('User Trends')
        # Display the interactive Plotly chart in Streamlit
        st.plotly_chart(fig, use_container_width=True)

        # Define a second query to retrieve sales data for the pie chart
        sales_query = f"""
        SELECT 
        masterCategory, 
        subCategory, 
        SUM(price_usd * quantity) AS total_sales
        FROM {bigquery_tbl}
        WHERE DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND event_name = 'BOOKING'  -- Filter to consider only purchase events
        GROUP BY masterCategory, subCategory
        ORDER BY total_sales DESC;
        """

        # Execute the sales query and get the results in a DataFrame
        sales_df = client.query(sales_query).to_dataframe()

        treemap = px.treemap(
            sales_df,
            path=["masterCategory", "subCategory"],
            values="total_sales",
            color="total_sales",
            color_continuous_scale="Tealgrn",
            labels={
                "masterCategory": "Main Category",
                "subCategory": "Sub Category",
                "total_sales": "Total Sales",
            }
        )

        # Customize the treemap layout
        treemap.update_layout(
            margin=dict(t=50, l=25, r=25, b=25),
            coloraxis_colorbar=dict(title="Total Sales"),
        )
        st.subheader('Sales by Product Category (Treemap View)')
        # Display the interactive treemap in Streamlit
        st.plotly_chart(treemap, use_container_width=True)

        top_products_query = f"""
        SELECT 
        productDisplayName, 
        SUM(quantity) AS total_quantity, 
        SUM(price_usd * quantity) AS total_sales,
        masterCategory,
        subCategory
        FROM {bigquery_tbl}
        WHERE DATE(event_time) BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND event_name = 'BOOKING'
        GROUP BY productDisplayName, masterCategory, subCategory
        ORDER BY total_sales DESC
        LIMIT 25;
        """

        # Execute the query and get the results in a DataFrame
        top_products_df = client.query(top_products_query).to_dataframe()

        # Create a bubble chart using Plotly
        bubble_chart = px.scatter(
            top_products_df,
            x="total_quantity",
            y="total_sales",
            size="total_sales",  # Bubble size by total sales
            color="subCategory",  # Color by sub-category
            hover_name="productDisplayName",  # Show product name on hover
            labels={
                "total_sales": "Total Sales ($)",
                "total_quantity": "Total Quantity Sold",
                "subCategory": "Product Sub-Category",
            },
            size_max=30  # Set maximum bubble size
        )

        # Customize the layout
        bubble_chart.update_layout(
            height=500,
            xaxis_title="Total Quantity Sold",
            yaxis_title="Total Sales ($)",
            yaxis=dict(
                range=[
                    top_products_df["total_sales"].min() - 50,
                    top_products_df["total_sales"].max() + 50,
                ],  # Ensure Y-axis starts from 0
                tick0=0,  # Start ticks from 0
                dtick=50,
            ),
            xaxis=dict(
                range=[
                    top_products_df["total_quantity"].min() - 5,
                    top_products_df["total_quantity"].max() + 5,
                ],  # Ensure Y-axis starts from 0
                tick0=0,  # Start ticks from 0
                dtick=5,
            ),
            legend_title="Product Sub-Category",
        )
        st.subheader('Top 50 Sold Products (Bubble Chart)')
        # Display the interactive bubble chart in Streamlit
        st.plotly_chart(bubble_chart, use_container_width=True)

        top_products_map_query = f"""
        SELECT 
        home_location_lat,home_location_long, 
        round(SUM(price_usd * quantity),2) AS total_sales
        FROM {bigquery_tbl}
        WHERE DATE(event_time) BETWEEN '2024-09-27' AND '2024-10-04'
        AND event_name = 'BOOKING'
        GROUP BY home_location_lat,home_location_long;
        """
        geo_df = client.query(top_products_map_query).to_dataframe()

        st.subheader('Geographical Sales')
        st.map(
            geo_df,
            latitude="home_location_lat",
            longitude="home_location_long",
            size="total_sales",
            zoom = 2
        )


# Clear the placeholders and render the appropriate page content
dashboard_placeholder.empty()
insights_placeholder.empty()
if page == "Realtime Dashboard":
    render_dashboard()
elif page == "Historic Dashboard":
    render_insights()
