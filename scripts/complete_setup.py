"""
Complete Data Lake Setup Script
Automates the entire data lake pipeline: Bronze → Silver → HDFS → Gold
"""

from minio import Minio
from hdfs import InsecureClient
import os
import sys
from pathlib import Path

# Configuration
MINIO_ENDPOINT = "localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "root"

# Data paths
DATA_DIR = Path("./data")
RAW_FILES = [
    "london_weather_dataset2.csv",
    "london_traffic_dataset2.csv"
]
SILVER_FILES = [
    "weather_cleaned.parquet",
    "traffic_cleaned.parquet",
    "merged_data (3).parquet"
]
GOLD_FILES = [
    "factor_loadings.csv",
    "Factor Analysis on Urban Traffic and Weather Data.docx",
    "final_factor_scores_gold_layer_4_factors (1).csv"
]


class DataLakeManager:
    """Manages the complete data lake pipeline"""
    
    def __init__(self):
        """Initialize MinIO and HDFS clients"""
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        self.hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
    
    def upload_to_bronze(self):
        """Upload new raw CSV files to Bronze"""
        print("\n" + "=" * 60)
        print("PHASE 1: Uploading Raw Data to Bronze")
        print("=" * 60)
        
        success_count = 0
        
        for filename in RAW_FILES:
            file_path = DATA_DIR / filename
            
            if not file_path.exists():
                print(f"File not found: {filename}")
                continue
            
            try:
                file_size = file_path.stat().st_size / (1024 * 1024)
                print(f"\n {filename}")
                print(f"   Size: {file_size:.2f} MB")
                print(f"   Uploading to bronze/{filename}...")
                
                self.minio_client.fput_object(
                    "bronze",
                    filename,
                    str(file_path)
                )
                
                print(f"Uploaded successfully")
                success_count += 1
                
            except Exception as e:
                print(f"Error: {e}")
        
        print(f"\n Uploaded {success_count}/{len(RAW_FILES)} files to Bronze")
        return success_count == len(RAW_FILES)
    
    def upload_to_silver(self):
        """Upload cleaned Parquet files to Silver"""
        print("\n" + "=" * 60)
        print(" PHASE 2: Uploading Cleaned Data to Silver")
        print("=" * 60)
        
        success_count = 0
        
        for filename in SILVER_FILES:
            file_path = DATA_DIR / filename
            
            if not file_path.exists():
                print(f" File not found: {filename}")
                continue
            
            try:
                file_size = file_path.stat().st_size / (1024 * 1024)
                print(f"\n {filename}")
                print(f"   Size: {file_size:.2f} MB")
                print(f"   Uploading to silver/{filename}...")
                
                self.minio_client.fput_object(
                    "silver",
                    filename,
                    str(file_path)
                )
                
                print(f" Uploaded successfully")
                success_count += 1
                
            except Exception as e:
                print(f"Error: {e}")
        
        print(f"\n Uploaded {success_count}/{len(SILVER_FILES)} files to Silver")
        return success_count == len(SILVER_FILES)
    
    def copy_to_hdfs(self):
        """Copy cleaned data from Silver to HDFS"""
        print("\n" + "=" * 60)
        print("PHASE 3: Copying Silver Data to HDFS")
        print("=" * 60)
        
        # Create HDFS directory structure
        hdfs_dirs = [
            "/bigdata/weather",
            "/bigdata/traffic",
            "/bigdata/merged"
        ]
        
        for directory in hdfs_dirs:
            try:
                if not self.hdfs_client.status(directory, strict=False):
                    self.hdfs_client.makedirs(directory)
                    print(f" Created HDFS directory: {directory}")
            except:
                try:
                    self.hdfs_client.makedirs(directory)
                    print(f" Created HDFS directory: {directory}")
                except Exception as e:
                    print(f" Directory already exists or error: {directory}")
        
        # Upload weather data
        print("\n Uploading weather_cleaned.parquet to HDFS...")
        try:
            with open(DATA_DIR / "weather_cleaned.parquet", "rb") as f:
                self.hdfs_client.write("/bigdata/weather/weather_cleaned.parquet", f, overwrite=True)
            print(" Weather data uploaded to HDFS")
        except Exception as e:
            print(f" Error: {e}")
        
        # Upload traffic data
        print("\n Uploading traffic_cleaned.parquet to HDFS...")
        try:
            with open(DATA_DIR / "traffic_cleaned.parquet", "rb") as f:
                self.hdfs_client.write("/bigdata/traffic/traffic_cleaned.parquet", f, overwrite=True)
            print(" Traffic data uploaded to HDFS")
        except Exception as e:
            print(f"Error: {e}")
        
        # Upload merged data
        print("\n Uploading merged_data.parquet to HDFS...")
        try:
            with open(DATA_DIR / "merged_data (3).parquet", "rb") as f:
                self.hdfs_client.write("/bigdata/merged/merged_data.parquet", f, overwrite=True)
            print(" Merged data uploaded to HDFS")
        except Exception as e:
            print(f"Error: {e}")
        
        print("\n HDFS integration complete")
        return True
    
    def upload_to_gold(self):
        """Upload final results to Gold bucket"""
        print("\n" + "=" * 60)
        print("PHASE 4: Uploading Results to Gold")
        print("=" * 60)
        
        success_count = 0
        
        for filename in GOLD_FILES:
            file_path = DATA_DIR / filename
            
            if not file_path.exists():
                print(f" File not found: {filename}")
                continue
            
            try:
                file_size = file_path.stat().st_size / (1024 * 1024)
                print(f"\n {filename}")
                print(f"   Size: {file_size:.2f} MB")
                print(f"   Uploading to gold/{filename}...")
                
                self.minio_client.fput_object(
                    "gold",
                    filename,
                    str(file_path)
                )
                
                print(f" Uploaded successfully")
                success_count += 1
                
            except Exception as e:
                print(f" Error: {e}")
        
        print(f"\n Uploaded {success_count}/{len(GOLD_FILES)} files to Gold")
        return success_count > 0
    
    def verify_setup(self):
        """Verify all buckets and HDFS structure"""
        print("\n" + "=" * 60)
        print("PHASE 5: Verifying Data Lake Setup")
        print("=" * 60)
        
        # Verify MinIO buckets
        for bucket_name in ["bronze", "silver", "gold"]:
            print(f"\n {bucket_name.upper()} Bucket:")
            try:
                objects = list(self.minio_client.list_objects(bucket_name, recursive=True))
                if objects:
                    for obj in objects:
                        size_mb = obj.size / (1024 * 1024)
                        print(f"   ✅ {obj.object_name} ({size_mb:.2f} MB)")
                else:
                    print("(Empty)")
            except Exception as e:
                print(f"  Error: {e}")
        
        # Verify HDFS
        print("\n📁 HDFS Structure:")
        try:
            for path in ["/bigdata/weather", "/bigdata/traffic", "/bigdata/merged"]:
                files = self.hdfs_client.list(path)
                print(f"   {path}:")
                for file in files:
                    print(f"      ✅ {file}")
        except Exception as e:
            print(f"Error: {e}")
        
        print("\n" + "=" * 60)
        print("Verification Complete!")
        print("=" * 60)


def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("BIG DATA PROJECT - DATA LAKE SETUP")
    print("Weather Impact on Urban Traffic Analysis")
    print("=" * 60)
    
    manager = DataLakeManager()
    
    # Execute phases
    try:
        # Phase 1: Upload raw data
        if not manager.upload_to_bronze():
            print("\n Warning: Some raw files failed to upload")
        
        # Phase 2: Upload cleaned data
        if not manager.upload_to_silver():
            print("\n Warning: Some cleaned files failed to upload")
        
        # Phase 3: Copy to HDFS
        manager.copy_to_hdfs()
        
        # Phase 4: Upload results
        manager.upload_to_gold()
        
        # Phase 5: Verify everything
        manager.verify_setup()
        
        print("\n" + "=" * 60)
        print(" DATA LAKE SETUP COMPLETE!")
        print("=" * 60)
        print("\nData Lake Architecture:")
        print(" Bronze → Raw CSV files (secured)")
        print(" Silver → Cleaned Parquet files")
        print("  HDFS   → Distributed storage (organized)")
        print("  Gold   → Factor Analysis results")
        print("\n All data securely stored and organized!")
        
    except Exception as e:
        print(f"\n Fatal Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
