from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# PostgreSQL connection ID configured in Airflow
PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def fetch_order_data():
    hook=PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn=hook.get_conn()

    query="""
            SELECT 
            o.orderDate :: date AS sales_date,
            od.productID,
            p.productName,
            od.quantity,
            p.price
            FROM orders o
        JOIN order_details od ON o.orderID = od.orderID
        JOIN products p ON od.productID = p.productID
        """
    df=pd.read_sql(query,conn)
    df.to_csv('/home/kiwilytics/airflow_output/sales_data.csv',index=False)
def process_daily_revenue():
    df=pd.read_csv('/home/kiwilytics/airflow_output/sales_data.csv')
    df['sales_date']=pd.to_datetime(df['sales_date'],errors="coerce")
    df['total_revenue']= df['quantity'] * df['price']
    revenue_per_day = df.groupby('sales_date').agg(total_revenue=('total_revenue','sum')).reset_index()
    revenue_per_day.to_csv('/home/kiwilytics/airflow_output/daily_revenue.csv')

def plot_daily_revenue():
    df=pd.read_csv('/home/kiwilytics/airflow_output/daily_revenue.csv')
    df['sales_date']=pd.to_datetime(df['sales_date'],errors="coerce")
    plt.figure(figsize=(12,6))
    plt.plot(df['sales_date'],df['total_revenue'],marker='o',linestyle='-')
    output_path='/home/kiwilytics/airflow_output/daily_revenue_plot.png'
    plt.savefig(output_path)
    print(f"Revenue chart saved to {output_path}")


with DAG(
    dag_id='daily_sales_revenue_analysis',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='Compute and visualize daily revenue using pandas and matplotlib in airflow',
) as dag:

    t1 = PythonOperator(
        task_id='fetch_order_data',
        python_callable=fetch_order_data
    )

    t2 = PythonOperator(
        task_id='process_daily_revenue',
        python_callable=process_daily_revenue
    )
    t3=  PythonOperator(
        task_id='plot_daily_revenue',
        python_callable=plot_daily_revenue,
    )

    t1 >> t2>>t3