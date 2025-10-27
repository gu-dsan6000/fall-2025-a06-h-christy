#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution Analysis
"""

import os
import sys
import time
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, count, to_timestamp, input_file_name
)
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""
    
    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        
        # Cluster Configuration
        .master(master_url)
        
        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        
        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        
        # S3 Configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        
        # Performance settings
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Arrow optimization
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        .getOrCreate()
    )
    
    logger.info("Spark session created successfully for cluster execution")
    return spark


def solve_problem1(spark, s3_path):
    """
    Solve Problem 1: Analyze log level distribution.
    """
    
    logger.info("Starting Problem 1: Log Level Distribution Analysis")
    print("\nSolving Problem 1: Log Level Distribution Analysis")
    print("=" * 60)
    
    start_time = time.time()
    
    # Step 1: Read all log files
    logger.info(f"Reading log files from S3: {s3_path}")
    print(f"Step 1: Reading log files from S3...")
    print(f"  Path: {s3_path}")
    
    logs_df = spark.read.text(s3_path)
    total_lines = logs_df.count()

    logger.info(f"Successfully loaded {total_lines:,} lines")
    print(f"✅ Successfully loaded {total_lines:,} lines")
    
    # Step 2: Parse log entries
    logger.info("Step 2: Extracting log levels from log entries")
    print("\nStep 2: Extracting log levels...")

    logs_parsed = (
        logs_df
        .filter(
            logs_df.value.rlike(r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}')
        )
    )
    logs_parsed = (
        logs_parsed
        .select(
            regexp_extract(
                'value',
                r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})',
                1
            )
            .alias('timestamp'),
            regexp_extract(
                'value',
                r'(INFO|WARN|ERROR|DEBUG)',
                1
            )
            .alias('log_level'),
            regexp_extract(
                'value',
                r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):',
                2
            )
            .alias('component'),
            col('value').alias('message')
        )
    )
    logs_parsed = (
        logs_parsed
        .withColumn(
            'timestamp', to_timestamp(col('timestamp'), 'yy/MM/dd HH:mm:ss')
        )
    )
    
    total_logs = logs_parsed.count()
    logger.info(f"Total log entries processed {total_logs:,}")
    print(f"Total log entries processed {total_logs:,}")
    
    # Step 3: Distribution of log levels
    logger.info("Step 3: Counting entries by log level")
    print("\nStep 3: Counting entries by log level...")
    
    counts_df = (
        logs_parsed
        .groupBy('log_level')
        .count()
        .orderBy('count', ascending=False)
    )
    
    print("\nLog level distribution:")
    counts_df.show()
    
    # Step 4: Randomly sample 10 log entries with their levels
    logger.info("Step 4: Randomly sample 10 log entries with their levels")
    print("\nStep 4: Randomly sample 10 log entries with their levels...")
    
    samples_df = (
        logs_parsed
        .select('message', 'log_level')
        .sample(withReplacement=False, fraction=0.005, seed = 6000)
        .limit(10)
    )
    
    print("\nSamples:")
    samples_df.show(truncate=False)
    
    # Step 5: Save outputs
    logger.info("Step 5: Saving outputs")
    print("\nStep 5: Saving outputs...")
    
    pd_counts_df = counts_df.toPandas()
    counts_fpath = "problem1_counts.csv"
    pd_counts_df.to_csv(counts_fpath, index=False)
    logger.info(f"Saved counts to {counts_fpath}")
    print(f"✅ Saved counts to {counts_fpath}")

    pd_samples_df = samples_df.toPandas()
    sample_fpath = "problem1_sample.csv"
    pd_samples_df.to_csv(sample_fpath, index=False)
    logger.info(f"Saved samples to {sample_fpath}")
    print(f"✅ Saved samples to {sample_fpath}")
    
    summary_fpath = "problem1_summary.txt"
    num_levels = counts_df.count()
    with open(summary_fpath, "w") as f:
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Total lines with log levels: {total_logs:,}\n")
        f.write(f"Unique log levels found   : {num_levels}\n")
        
        f.write("Log level distribution:\n")
        for i, j in pd_counts_df.iterrows():
            pct = (j['count'] / total_logs) * 100
            f.write(f"  {j['log_level']:<6}: {j['count']:>8,} ({pct:5.2f}%)\n")
    
    logger.info(f"Saved summary to {summary_fpath}")
    print(f"✅ Saved summary to {summary_fpath}")
    
    end_time = time.time()
    exec_time = end_time - start_time
    logger.info(f"Problem 1 Execution time: {exec_time:.2f} seconds")
    
    print("\n" + "=" * 60)
    print("\nProblem1 Summary")
    print(f"Total log lines processed : {total_lines:,}")
    print(f"Total lines with log levels: {total_logs:,}")
    print(f"Unique log levels found   : {num_levels}")
    print("\nLog level distribution:")
    for i, j in pd_counts_df.iterrows():
        pct = (j['count'] / total_logs) * 100
        print(f"  {j['log_level']:<6}: {j['count']:>8,} ({pct:5.2f}%)")
    print(f"Execution time: {exec_time:.2f} seconds")
    
    return counts_df, samples_df


def main():
    logger.info("Starting Problem 1: Log Level Distribution Analysis")
    print("=" * 70)
    print("PROBLEM 1: Log Level Distribution Analysis")
    print("Spark Cluster Log Analysis")
    print("=" * 70)
    
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    parser.add_argument("master_url", nargs="?", help="master url")
    parser.add_argument("--net-id", required=True, help="net id")
    args = parser.parse_args()
    
    # Get master url
    if args.master_url:
        master_url = args.master_url
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("Usage: python problem1.py spark://MASTER_IP:7077 --net-id YOUR-NET-ID")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1
    
    # Construct S3 path
    s3_bucket = f"s3a://{args.net_id}-assignment-spark-cluster-logs"
    s3_path = f"{s3_bucket}/data/application_*/*.log"
    
    print(f"Connecting to Spark Master at: {master_url}")
    print(f"Reading data from: {s3_path}")
    logger.info(f"Using Spark master URL: {master_url}")
    logger.info(f"Using S3 path: {s3_path}")
    
    overall_start = time.time()
    
    # Create Spark session
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)
    
    # Solve Problem 1
    try:
        logger.info("Starting Problem 1 analysis")
        counts_df, sample_df = solve_problem1(spark, s3_path)
        success = True
        logger.info("Problem 1 analysis completed successfully")
    except Exception as e:
        logger.exception(f"Error occurred while solving Problem 1: {str(e)}")
        print(f"❌ Error solving Problem 1: {str(e)}")
        success = False
    
    # Clean up
    logger.info("Stopping Spark session")
    spark.stop()
    
    # Overall timing
    overall_end = time.time()
    total_time = overall_end - overall_start
    logger.info(f"Execution time: {total_time:.2f} seconds")
    
    print("\n" + "=" * 70)
    if success:
        print("✅ PROBLEM 1 COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        print("\nFiles created:")
        print("  - problem1_counts.csv")
        print("  - problem1_sample.csv")
        print("  - problem1_summary.txt")
        print("\nNext steps:")
        print("  1. Download files")
    else:
        print("❌ Problem 1 failed - check error messages above")
    print("=" * 70)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())