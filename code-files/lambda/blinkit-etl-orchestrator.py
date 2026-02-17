import boto3
import json
import os
import time
from datetime import datetime

glue_client = boto3.client('glue')
CRAWLER_NAME = os.environ.get('CRAWLER_NAME', '')
GLUE_JOBS = {
    'sales_revenue': os.environ.get('JOB_SALES_REVENUE', ''),
    'delivery_performance': os.environ.get('JOB_DELIVERY_PERFORMANCE', ''),
    'customer_feedback': os.environ.get('JOB_CUSTOMER_FEEDBACK', ''),
    'marketing_roi': os.environ.get('JOB_MARKETING_ROI', ''),
    'inventory_reconciliation': os.environ.get('JOB_INVENTORY_RECONCILIATION', '')
}

def lambda_handler(event, context):
    try:
        print("Event received")
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            if not key.lower().endswith('.csv'):
                print(f"Skipping: {key}")
                continue
            print(f"Processing: s3://{bucket}/{key}")
            jobs_to_run = determine_jobs_to_run(key)
            print(f"Jobs: {jobs_to_run}")
            if not jobs_to_run:
                continue
            print(f"Running {len(jobs_to_run)} jobs...")
            job_results = run_glue_jobs_sequential(jobs_to_run)
            successful = [j for j in job_results if job_results[j]['status'] == 'success']
            failed = [j for j in job_results if job_results[j]['status'] == 'failed']
            print(f"Successful: {len(successful)}, Failed: {len(failed)}")
            if len(successful) > 0:
                print("Starting crawler...")
                start_and_wait_crawler()
            return {'statusCode': 200 if len(failed) == 0 else 207, 'body': json.dumps({'message': 'Complete', 'successful': len(successful), 'failed': len(failed)})}
        return {'statusCode': 200, 'body': 'No CSV files'}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def determine_jobs_to_run(file_key):
    file_name = file_key.split('/')[-1].lower()
    jobs = []
    if any(k in file_name for k in ['order', 'product']): jobs.extend(['sales_revenue', 'delivery_performance'])
    if 'feedback' in file_name: jobs.append('customer_feedback')
    if 'marketing' in file_name: jobs.append('marketing_roi')
    if 'inventory' in file_name: jobs.append('inventory_reconciliation')
    if not jobs: jobs = list(GLUE_JOBS.keys())
    return list(set(jobs))

def run_glue_jobs_sequential(jobs_to_run):
    results = {}
    for idx, job_key in enumerate(jobs_to_run, 1):
        job_name = GLUE_JOBS.get(job_key)
        if not job_name:
            results[job_key] = {'status': 'failed', 'error': 'Not configured'}
            continue
        try:
            response = glue_client.start_job_run(JobName=job_name, Arguments={'--S3_RAW_BUCKET': os.environ.get('S3_RAW_BUCKET', ''), '--S3_PROCESSED_BUCKET': os.environ.get('S3_PROCESSED_BUCKET', ''), '--S3_CURATED_BUCKET': os.environ.get('S3_CURATED_BUCKET', '')})
            job_run_id = response['JobRunId']
            print(f"Job {idx}: {job_name} started - {job_run_id}")
            start_time = time.time()
            while (time.time() - start_time) < 600:
                resp = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
                status = resp['JobRun']['JobRunState']
                if status == 'SUCCEEDED':
                    results[job_key] = {'status': 'success'}
                    break
                elif status in ['FAILED', 'TIMEOUT', 'STOPPED']:
                    results[job_key] = {'status': 'failed', 'error': status}
                    break
                time.sleep(20)
            else:
                results[job_key] = {'status': 'failed', 'error': 'Timeout'}
        except Exception as e:
            results[job_key] = {'status': 'failed', 'error': str(e)}
    return results

def start_and_wait_crawler():
    try:
        try:
            glue_client.start_crawler(Name=CRAWLER_NAME)
        except:
            pass
        start_time = time.time()
        while (time.time() - start_time) < 300:
            resp = glue_client.get_crawler(Name=CRAWLER_NAME)
            if resp['Crawler']['State'] == 'READY':
                return {'status': 'success'}
            time.sleep(20)
        return {'status': 'timeout'}
    except:
        return {'status': 'failed'}