"""
Upload files to HDFS using Docker exec
This script uploads files directly through the HDFS container
"""

import subprocess
import os
from pathlib import Path

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

for local_file, hdfs_path in files_to_upload:
    local_path = DATA_DIR / local_file
    
    if not local_path.exists():
        print(f"File not found: {local_file}")
        continue
    
    print(f"Uploading {local_file} to HDFS...")
    print(f"   Local: {local_path}")
    print(f"   HDFS: {hdfs_path}")
    
    try:
        # Copy file into container
        copy_cmd = f'docker cp "{local_path}" hdfs-namenode:/tmp/{local_file}'
        result = subprocess.run(copy_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"   Failed to copy to container: {result.stderr}")
            continue
        
        # Put file into HDFS
        put_cmd = f'docker exec hdfs-namenode hdfs dfs -put -f /tmp/{local_file} {hdfs_path}'
        result = subprocess.run(put_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"   Failed to upload to HDFS: {result.stderr}")
            continue
        
        print(f"   Uploaded successfully")
        
    except Exception as e:
        print(f"   Error: {e}")
    
    print()

# Verify uploads
print("=" * 60)
print("Verifying HDFS Contents")
print("=" * 60)
print()

for path in ["/bigdata/weather", "/bigdata/traffic", "/bigdata/merged"]:
    print(f"{path}:")
    list_cmd = f'docker exec hdfs-namenode hdfs dfs -ls {path}'
    result = subprocess.run(list_cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        lines = result.stdout.strip().split('\n')
        for line in lines[1:]:  # Skip header
            if line.strip():
                parts = line.split()
                if len(parts) >= 8:
                    filename = parts[-1]
                    size = parts[4]
                    print(f"   {filename.split('/')[-1]} ({size} bytes)")
    else:
        print(f"   (Empty or error)")
    print()

print("=" * 60)
print(" HDFS Upload Complete!")
print("=" * 60)
