from minio import Minio
import subprocess
import os
import sys
from pathlib import Path

# Configuration
MINIO_ENDPOINT = "localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

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
        """Initialize MinIO client"""
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
    
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
    
    def upload_to_gold(self):
        """Upload final results to Gold bucket"""
        print("\n" + "=" * 60)
        print("PHASE 3: Uploading Results to Gold")
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
        print("PHASE 4: Verifying Data Lake Setup")
        print("=" * 60)
        
        # Verify MinIO buckets
        for bucket_name in ["bronze", "silver", "gold"]:
            print(f"\n {bucket_name.upper()} Bucket:")
            try:
                objects = list(self.minio_client.list_objects(bucket_name, recursive=True))
                if objects:
                    for obj in objects:
                        size_mb = obj.size / (1024 * 1024)
                        print(f" {obj.object_name} ({size_mb:.2f} MB)")
                else:
                    print("(Empty)")
            except Exception as e:
                print(f"  Error: {e}")
        
        # Verify HDFS (using docker exec)
        print("\n HDFS Structure:")
        try:
            result = subprocess.run(
                ["docker", "exec", "hdfs-namenode", "hdfs", "dfs", "-ls", "-R", "/bigdata"],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip():
                        print(f"   {line}")
            else:
                print("   HDFS not yet populated (run upload_to_hdfs.py)")
        except Exception as e:
            print(f"   Error: {e}")
        
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
        
        # Phase 3: Upload results to Gold
        manager.upload_to_gold()
        
        # Phase 4: Verify everything
        manager.verify_setup()
        
        print("\n" + "=" * 60)
        print("NOTE: To copy data to HDFS, run:")
        print("      python scripts/upload_to_hdfs.py")
        print("=" * 60)
        
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
