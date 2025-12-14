
import subprocess
import os
import sys
from pathlib import Path


"""
Upload files to HDFS using Docker exec
This script uploads files directly through the HDFS container
"""

DATA_DIR = Path("./data")

files_to_upload = [
    ("weather_cleaned.parquet", "/bigdata/weather/weather_cleaned.parquet"),
    ("traffic_cleaned.parquet", "/bigdata/traffic/traffic_cleaned.parquet"),
    ("merged_data (3).parquet", "/bigdata/merged/merged_data.parquet")
]

print("=" * 60)
print("HDFS Upload via Docker Exec")
print("=" * 60)
print()

# Check if Docker container is running
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

# Create HDFS directories first
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

# Upload files
success_count = 0
for local_file, hdfs_path in files_to_upload:
    local_path = DATA_DIR / local_file
    
    if not local_path.exists():
        print(f"File not found: {local_file}")
        print(f"Expected at: {local_path}")
        continue
    
    file_size = local_path.stat().st_size / 1024  # KB
    print(f"   Uploading {local_file}")
    print(f"   Local: {local_path}")
    print(f"   Size: {file_size:.2f} KB")
    print(f"   HDFS: {hdfs_path}")
    
    try:
        # Step 1: Copy file into container
        temp_name = local_file.replace(" ", "_").replace("(", "").replace(")", "")
        copy_cmd = f'docker cp "{local_path}" hdfs-namenode:/tmp/{temp_name}'
        result = subprocess.run(copy_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f" Failed to copy to container: {result.stderr}")
            continue
        
        # Step 2: Put file into HDFS
        put_cmd = f'docker exec hdfs-namenode hdfs dfs -put -f /tmp/{temp_name} {hdfs_path}'
        result = subprocess.run(put_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f" Failed to upload to HDFS: {result.stderr}")
            continue
        
        # Step 3: Clean up temp file
        clean_cmd = f'docker exec hdfs-namenode rm /tmp/{temp_name}'
        subprocess.run(clean_cmd, shell=True, capture_output=True, text=True)
        
        print(f" Uploaded successfully")
        success_count += 1
        
    except Exception as e:
        print(f"Error: {e}")
    
    print()

print("=" * 60)
print(f"Upload Summary: {success_count}/{len(files_to_upload)} files uploaded")
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

print("=" * 60)
if success_count == len(files_to_upload):
    print("HDFS Upload Complete! All files uploaded successfully.")
else:
    print(f"HDFS Upload Completed with warnings.")
    print(f"{success_count}/{len(files_to_upload)} files uploaded successfully")
print("=" * 60)
