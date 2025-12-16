import subprocess
import os
import sys
from pathlib import Path
from minio import Minio

"""
Upload files from MinIO to HDFS
This script downloads files from MinIO Silver bucket and uploads them to HDFS
"""

# --- Configuration ---
MINIO_ENDPOINT = "localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
SECURE_CONNECTION = False

# Mapping: (MinIO Bucket, MinIO Object Name, HDFS Destination Path)
FILES_MAPPING = [
    ("silver", "weather_cleaned.parquet", "/bigdata/weather/weather_cleaned.parquet"),
    ("silver", "traffic_cleaned.parquet", "/bigdata/traffic/traffic_cleaned.parquet"),
    ("silver", "merged_data (3).parquet", "/bigdata/merged/merged_data.parquet")
]

print("=" * 60)
print("Pipeline: MinIO Silver -> Local Temp -> HDFS")
print("=" * 60)
print()

# 1. Initialize MinIO Client
print("Connecting to MinIO...")
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=SECURE_CONNECTION
    )
    # Test connection
    if not minio_client.bucket_exists("silver"):
        print("Error: Bucket 'silver' does not exist in MinIO!")
        sys.exit(1)
    print("Connected to MinIO successfully.")
except Exception as e:
    print(f"Error connecting to MinIO: {e}")
    sys.exit(1)
print()

# 2. Check if Docker HDFS container is running
print("Checking HDFS container...")
check_cmd = "docker ps --filter name=hdfs-namenode --format {{.Names}}"
try:
    result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
    if "hdfs-namenode" not in result.stdout:
        print("Error: HDFS NameNode container is not running!")
        print("Please start containers first: docker-compose up -d")
        sys.exit(1)
    print("HDFS container is running")
    print()
except Exception as e:
    print(f"Error checking Docker: {e}")
    sys.exit(1)

# 3. Create HDFS directories first
print("Creating HDFS directory structure...")
directories = ["/bigdata", "/bigdata/weather", "/bigdata/traffic", "/bigdata/merged"]

for directory in directories:
    mkdir_cmd = f'docker exec hdfs-namenode hdfs dfs -mkdir -p {directory}'
    result = subprocess.run(mkdir_cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Created/verified: {directory}")
    else:
        if "File exists" not in result.stderr:
            print(f"Warning for {directory}: {result.stderr.strip()}")

print()

# 4. Process Files (Download form MinIO -> Upload to HDFS)
success_count = 0

for bucket, object_name, hdfs_path in FILES_MAPPING:
    print(f"Processing: {object_name}")
    
    # Define a temporary local filename
    local_temp_file = f"temp_{object_name}"
    
    try:
        # --- Step A: Download from MinIO ---
        print(f"   1. Downloading from MinIO ({bucket})...")
        minio_client.fget_object(bucket, object_name, local_temp_file)
        
        file_size = os.path.getsize(local_temp_file) / 1024 # KB
        print(f"      Downloaded. Size: {file_size:.2f} KB")

        # --- Step B: Upload to HDFS (via Docker) ---
        print(f"   2. Uploading to HDFS...")
        
        # Sanitize filename for Docker command (remove spaces/parentheses)
        safe_temp_name = object_name.replace(" ", "_").replace("(", "").replace(")", "")
        
        # B1: Copy from Local Host to Docker Container (/tmp)
        copy_cmd = f'docker cp "{local_temp_file}" hdfs-namenode:/tmp/{safe_temp_name}'
        result = subprocess.run(copy_cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Docker Copy Failed: {result.stderr}")
            
        # B2: Put from Container /tmp to HDFS
        put_cmd = f'docker exec hdfs-namenode hdfs dfs -put -f /tmp/{safe_temp_name} {hdfs_path}'
        result = subprocess.run(put_cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"HDFS Put Failed: {result.stderr}")
            
        # B3: Clean up inside Docker Container
        clean_cmd = f'docker exec hdfs-namenode rm /tmp/{safe_temp_name}'
        subprocess.run(clean_cmd, shell=True, capture_output=True, text=True)
        
        print(f"      Uploaded to HDFS successfully")
        success_count += 1

    except Exception as e:
        print(f"   Error: {e}")
    
    finally:
        # --- Step C: Clean up Local Temp File ---
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)
            print("   3. Local temp file cleaned up")
    
    print("-" * 40)

print()
print("=" * 60)
print(f"Pipeline Summary: {success_count}/{len(FILES_MAPPING)} files processed")
print("=" * 60)
print()

# Verify uploads
print("=" * 60)
print("Verifying HDFS Contents")
print("=" * 60)
print()

for path in ["/bigdata/weather", "/bigdata/traffic", "/bigdata/merged"]:
    print(f" {path}:")
    list_cmd = f'docker exec hdfs-namenode hdfs dfs -ls {path}'
    result = subprocess.run(list_cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        lines = result.stdout.strip().split('\n')
        found_files = False
        for line in lines:
            if line.strip() and not line.startswith('Found'):
                parts = line.split()
                if len(parts) >= 8:
                    filename = parts[-1]
                    size = parts[4]
                    print(f"{filename.split('/')[-1]} ({size} bytes)")
                    found_files = True
        if not found_files:
            print(f"   (Empty)")
    else:
        print(f"Error: {result.stderr.strip()}")
    print()