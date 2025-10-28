#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
"""

import os
import sys
import time
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, count, min as spark_min, max as spark_max,
    input_file_name, to_timestamp
)
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)

def create_spark_session(master_url):
    
    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsageAnalysis")
        
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
    
    logger.info("Spark session created successfully")
    return spark


def solve_problem2(spark, s3_path):
    """
    Problem 2: Cluster Usage Analysis
    """
    
    logger.info("Starting Problem 2: Cluster Usage Analysis")
    print("\nSolving Problem 2: Cluster Usage Analysis")
    print("=" * 60)
    
    start_time = time.time()
    
    # Step 1: Read all log files
    logger.info(f"Reading log files from S3: {s3_path}")
    print(f"Step 1: Reading log files from S3...")
    print(f"  Path: {s3_path}")
    
    logs_df = spark.read.text(s3_path)

    total_lines = logs_df.count()
    logger.info(f"Successfully loaded {total_lines:,} total log lines")
    print(f"✅ Loaded {total_lines:,} total log lines")
    
    # Create file_path column using input_file_name
    logs_df = logs_df.withColumn('file_path', input_file_name())
    
    # Step 2: Extract from file path
    logger.info("Step 2: Extracting ids")
    print("\nStep 2: Extracting ids...")
    
    logs_parsed = (
        logs_df
        .filter(col('value').rlike(r'^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}'))
        .withColumn("cluster_id", regexp_extract("file_path", r"application_(\d+)_(\d+)", 1))
        .withColumn("application_id", regexp_extract("file_path", r"application_(\d+_\d+)", 0))
        .withColumn("app_number", regexp_extract("file_path", r"application_\d+_(\d+)", 1))
        .filter((col("cluster_id") != "") & (col("application_id") != ""))
    )

    # cache intermediate dataframe
    logs_parsed.cache()
    total_processed = logs_parsed.count()
    logger.info(f"Successfully parsed {total_processed:,} log entries")
    print(f"✅ Parsed {total_processed:,} log entries")
    
    # Step 3: Extract timestamps from log entries
    logger.info("Step 3: Extracting timestamps from log entries")
    print("\nStep 3: Extracting timestamps...")
    
    time_df = (
        logs_parsed
        .select(
            col("cluster_id"),
            col("application_id"),
            col("app_number"),
            regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp0"),
        )
        .filter(col("timestamp0") != "")
        .withColumn("timestamp", to_timestamp("timestamp0", "yy/MM/dd HH:mm:ss"))
        .drop('timestamp0')
    )
    total_timestamps = time_df.count()
    print(f"✅ Filtered {total_timestamps:,} logs with timestamps")

    # Step 4: Application execution timelines
    logger.info("Step 4: Extracting application timelines")
    print("\nStep 4: Extracting application timelines...")
    
    app_df = (
        time_df
        .groupBy("cluster_id", "application_id", "app_number")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("cluster_id", "app_number")
    )

    # cache app_df
    app_df = app_df.cache()
    total_apps = app_df.count()
    logger.info(f"{total_apps} applications found")
    print(f"✅ {total_apps} applications found")
    
    print("\nDisplay application timelines:")
    app_df.show(n = 10, truncate=False)
    
    # Step 5: Summarize cluster usage
    logger.info("Step 5: Summarizing cluster usage")
    print("\nStep 5: Summarizing cluster usage...")
    
    cluster_df = (
        app_df
        .groupBy("cluster_id")
        .agg(
            count("*").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        )
        .orderBy("num_applications", ascending=False)
    )
    
    print("\nCluster usage summary:")
    cluster_df.show(truncate=False)
    
    # Step 6: Save outputs
    logger.info("Step 6: Saving outputs")
    print("\nStep 6: Saving results...")
    
    # out1
    pd_app_df = app_df.toPandas()
    app_fpath = "problem2_timeline.csv"
    pd_app_df.to_csv(app_fpath, index=False)
    logger.info(f"Saved timelines to {app_fpath}")
    print(f"✅ Saved timelines to {app_fpath}")
    
    # out2
    pd_cluster_df = cluster_df.toPandas()
    cluster_fpath = "problem2_cluster_summary.csv"
    pd_cluster_df.to_csv(cluster_fpath, index=False)
    logger.info(f"Saved cluster summary to {cluster_fpath}")
    print(f"✅ Saved cluster summary to {cluster_fpath}")
    
    # out3
    stats_fpath = "problem2_stats.txt"
    total_clusters = len(pd_cluster_df)
    avg_apps_cluster = total_apps / total_clusters if total_clusters > 0 else 0
    
    with open(stats_fpath, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_cluster:.2f}\n\n")
        
        f.write("Most heavily used clusters:\n")
        for i, j in pd_cluster_df.iterrows():
            f.write(f"  Cluster {j['cluster_id']}: {j['num_applications']} applications\n")
    
    logger.info(f"Saved overall statistics to {stats_fpath}")
    print(f"✅ Saved overall statistics to {stats_fpath}")
    
    end_time = time.time()
    exec_time = end_time - start_time
    logger.info(f"Problem 2 Execution time: {exec_time:.2f} seconds")
    
    # Print summary
    print("\n" + "=" * 60)
    print("PROBLEM 2 COMPLETED - Summary Statistics")
    print("=" * 60)
    print(f"Total unique clusters: {total_clusters}")
    print(f"Total applications: {total_apps}")
    print(f"Average apps per cluster: {avg_apps_cluster:.2f}")
    print(f"Execution time: {exec_time:.2f} seconds")
    print("\nMost heavily used clusters:")
    for i, j in pd_cluster_df.iterrows():
        print(f"  Cluster {j['cluster_id']}: {j['num_applications']} applications")
    
    return pd_app_df, pd_cluster_df


def plot_results(pd_app_df, pd_cluster_df):
    logger.info("Creating visualizations")
    print("\n" + "=" * 60)
    print("Creating Visualizations...")
    print("=" * 60)
    
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
    
    # Vis1
    logger.info("Creating bar chart showing applications per cluster")
    print("\nCreating bar chart showing applications per cluster...")
    
    pd_cluster_df = pd_cluster_df.sort_values('num_applications', ascending=False)
    plt.figure()
    ax = sns.barplot(
        x="cluster_id",
        y="num_applications",
        data=pd_cluster_df,
        hue="cluster_id",
        dodge=False,
        palette="Set2",
    )
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.xticks(rotation=45)

    for container in ax.containers:
        ax.bar_label(container, fmt="%d", label_type="edge", fontsize=9, padding=2)

    plt.tight_layout()

    barplot_fpath = "problem2_bar_chart.png"
    plt.savefig(barplot_fpath, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Saved bar chart to {barplot_fpath}")
    print(f"✅ Saved bar chart to {barplot_fpath}")
    
    # Viz2
    logger.info("Creating density plot: Job duration distribution")
    print("\nCreating density plot: Job duration distribution...")
    
    largest_cluster_id = pd_cluster_df.iloc[0]['cluster_id']
    largest_cluster_apps = pd_cluster_df.iloc[0]['num_applications']
    
    # largest
    df17 = pd_app_df[pd_app_df['cluster_id'] == largest_cluster_id].copy()
    
    df17['start_time'] = pd.to_datetime(df17['start_time'])
    df17['end_time'] = pd.to_datetime(df17['end_time'])
    df17['duration_minutes'] = (
        df17['end_time'] - df17['start_time']
    ).dt.total_seconds() / 60
    
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)

    df17 = df17[df17['duration_minutes'] > 0]
    
    plt.figure()
    sns.histplot(
        data=df17,
        x='duration_minutes',
        bins=40,
        kde=True,
        log_scale=True,
    )
    plt.title(f"Cluster {largest_cluster_id} Job Duration Distribution (n={largest_cluster_apps})")
    plt.xlabel("Duration log scale (minute)")
    plt.ylabel("Density")
    plt.tight_layout()
    hist_fpath = "problem2_density_plot.png"
    plt.savefig(hist_fpath, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Saved density plot to {hist_fpath}")
    print(f"✅ Saved density plot to {hist_fpath}")


def main():
    logger.info("Starting Problem 2: Cluster Usage Analysis")
    print("=" * 70)
    print("PROBLEM 2: Cluster Usage Analysis")
    print("=" * 70)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master_url", nargs="?", help="Spark master URL (spark://HOST:7077)")
    parser.add_argument("--net-id", required=True, help="Your net ID for S3 bucket name")
    parser.add_argument("--skip-spark", action="store_true", 
                        help="Skip Spark and only regenerate visualizations from existing CSVs")
    args = parser.parse_args()
    
    overall_start = time.time()
    
    if args.skip_spark:
        logger.info("Skipping Spark processing, loading existing CSVs")
        print("\nSkipping Spark processing - loading existing CSVs...")
        
        try:
            pd_app_df = pd.read_csv("problem2_timeline.csv")
            pd_cluster_df = pd.read_csv("problem2_cluster_summary.csv")
            print("✅ Loaded existing CSV files")
            
            # Create visualizations
            plot_results(pd_app_df, pd_cluster_df)
            success = True
        except FileNotFoundError as e:
            print(f"❌ Error: Could not find CSV files. Run without --skip-spark first.")
            print(f"   Missing file: {e.filename}")
            success = False
    else:
        # Get master URL
        if args.master_url:
            master_url = args.master_url
        else:
            master_private_ip = os.getenv("MASTER_PRIVATE_IP")
            if master_private_ip:
                master_url = f"spark://{master_private_ip}:7077"
            else:
                print("❌ Error: Master URL not provided")
                print("Usage: python problem2.py spark://MASTER_IP:7077 --net-id YOUR-NET-ID")
                print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
                print("\nTo skip Spark and regenerate visualizations:")
                print("   python problem2.py --skip-spark --net-id YOUR-NET-ID")
                return 1
        
        # Construct S3 path
        s3_bucket = f"s3a://{args.net_id}-assignment-spark-cluster-logs"
        s3_path = f"{s3_bucket}/data/application_*/*.log"
        
        print(f"Connecting to Spark Master at: {master_url}")
        print(f"Reading data from: {s3_path}")
        logger.info(f"Using Spark master URL: {master_url}")
        logger.info(f"Using S3 path: {s3_path}")
        
        # Create Spark session
        logger.info("Initializing Spark session for cluster execution")
        spark = create_spark_session(master_url)
        
        # Solve Problem 2
        try:
            logger.info("Starting Problem 2 analysis")
            pd_app_df, pd_cluster_df = solve_problem2(spark, s3_path)
            
            # Plot
            plot_results(pd_app_df, pd_cluster_df)
            
            success = True
            logger.info("Problem 2 analysis completed successfully")
        except Exception as e:
            logger.exception(f"Error occurred while solving Problem 2: {str(e)}")
            print(f"❌ Error solving Problem 2: {str(e)}")
            success = False
        
        # Clean up
        logger.info("Stopping Spark session")
        spark.stop()
    
    # Overall timing
    overall_end = time.time()
    total_time = overall_end - overall_start
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    
    print("\n" + "=" * 70)
    if success:
        print("✅ Problem 2 completed successfully!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        print("\nFiles created:")
        print("  - problem2_timeline.csv (Application timeline data)")
        print("  - problem2_cluster_summary.csv (Cluster statistics)")
        print("  - problem2_stats.txt (Summary statistics)")
        print("  - problem2_bar_chart.png (Bar chart visualization)")
        print("  - problem2_density_plot.png (Density plot visualization)")
        print("\nNext steps:")
        print("  1. Download")
    else:
        print("❌ Problem 2 failed - check error messages above")
    print("=" * 70)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())