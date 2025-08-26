from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import csv
import os
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_data(**context):
    """
    Example function that processes data using only built-in Python libraries
    """
    # Get execution date from context
    execution_date = context['execution_date']
    print(f"Execution date: {execution_date}")
    
    # Sample data processing
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    # Perform calculations
    total = sum(data)
    average = total / len(data)
    maximum = max(data)
    minimum = min(data)
    
    results = {
        'total': total,
        'average': average,
        'maximum': maximum,
        'minimum': minimum,
        'processed_at': datetime.now().isoformat()
    }
    
    print(f"Processing results: {results}")
    return results

def generate_report(**context):
    """
    Generate a simple report using built-in libraries
    """
    # Pull data from previous task
    ti = context['ti']
    processing_results = ti.xcom_pull(task_ids='process_data_task')
    
    # Create a simple report
    report = {
        'report_generated_at': datetime.now().isoformat(),
        'summary': f"Processed {processing_results['total']} items with average {processing_results['average']:.2f}",
        'details': processing_results
    }
    
    # Convert to JSON string for logging
    report_json = json.dumps(report, indent=2)
    print(f"Generated report:\n{report_json}")
    
    return report

def file_operations_example():
    """
    Example of file operations using built-in libraries
    """
    # Create sample data
    sample_data = [
        ['Name', 'Age', 'City'],
        ['Alice', '30', 'New York'],
        ['Bob', '25', 'London'],
        ['Charlie', '35', 'Tokyo']
    ]
    
    # Write to CSV (in Airflow's temporary directory)
    temp_dir = '/tmp/airflow_temp/'
    os.makedirs(temp_dir, exist_ok=True)
    
    csv_file = os.path.join(temp_dir, 'sample_data.csv')
    
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(sample_data)
    
    print(f"CSV file created at: {csv_file}")
    
    # Read and display the file content
    with open(csv_file, 'r') as file:
        content = file.read()
        print("File content:")
        print(content)
    
    return csv_file

def error_handling_demo():
    """
    Demonstration of error handling and logging
    """
    try:
        # Simulate some operation
        numbers = [1, 2, 3, 4, 5]
        
        # This will work
        result = sum(numbers) / len(numbers)
        print(f"Average: {result}")
        
        # Simulate a potential error scenario
        if result > 2:
            # Just a warning, not an error
            print("Warning: Average is greater than 2")
        
        return result
        
    except Exception as e:
        logging.error(f"Error in error_handling_demo: {e}")
        raise

def simple_data_transformation():
    """
    Simple data transformation using list comprehensions and built-in functions
    """
    # Sample data
    numbers = list(range(1, 21))
    
    # Transformations
    even_numbers = [x for x in numbers if x % 2 == 0]
    squared_numbers = [x**2 for x in numbers]
    number_pairs = [(x, x*2) for x in numbers if x <= 10]
    
    results = {
        'original_count': len(numbers),
        'even_count': len(even_numbers),
        'squared_sum': sum(squared_numbers),
        'pairs_example': number_pairs[:3]  # Show first 3 pairs
    }
    
    print(f"Transformation results: {results}")
    return results

# Define the DAG
with DAG(
    'arbitrary_python_code_dag',
    default_args=default_args,
    description='A DAG that runs arbitrary Python code using only built-in libraries',
    schedule=timedelta(hours=1),  # Changed from schedule_interval to schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'python'],
) as dag:

    start_task = PythonOperator(
        task_id='start',
        python_callable=lambda: print("DAG started successfully"),
    )
    
    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
    )
    
    generate_report_task = PythonOperator(
        task_id='generate_report_task',
        python_callable=generate_report,
    )
    
    file_ops_task = PythonOperator(
        task_id='file_operations_task',
        python_callable=file_operations_example,
    )
    
    data_transform_task = PythonOperator(
        task_id='data_transformation_task',
        python_callable=simple_data_transformation,
    )
    
    error_demo_task = PythonOperator(
        task_id='error_handling_demo_task',
        python_callable=error_handling_demo,
    )
    
    end_task = PythonOperator(
        task_id='end',
        python_callable=lambda: print("DAG completed successfully"),
    )

    # Define task dependencies
    start_task >> process_data_task >> generate_report_task
    start_task >> file_ops_task
    start_task >> data_transform_task
    start_task >> error_demo_task
    
    [generate_report_task, file_ops_task, data_transform_task, error_demo_task] >> end_task
