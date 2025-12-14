"""
Infrastructure Verification Script
Checks if all components are running correctly
"""

import sys
from minio import Minio
from hdfs import InsecureClient


def check_minio():
    """Check MinIO connection and buckets"""
    print("Checking MinIO...")
    try:
        client = Minio(
            "localhost:9002",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        
        # Check connection
        buckets = client.list_buckets()
        print("MinIO is accessible")
        
        # Check required buckets
        bucket_names = [b.name for b in buckets]
        required_buckets = ["bronze", "silver", "gold"]
        
        for bucket in required_buckets:
            if bucket in bucket_names:
                print(f" Bucket '{bucket}' exists")
            else:
                print(f" Bucket '{bucket}' is missing")
                return False
        
        return True
        
    except Exception as e:
        print(f" MinIO connection failed: {e}")
        return False


def check_hdfs():
    """Check HDFS connection and directories"""
    print("\n Checking HDFS...")
    try:
        client = InsecureClient("http://localhost:9870", user="root")
        
        # Check connection
        client.list("/")
        print(" HDFS is accessible")
        
        # Check required directories
        required_dirs = ["/bigdata", "/bigdata/weather", "/bigdata/traffic"]
        
        for directory in required_dirs:
            try:
                client.status(directory)
                print(f" Directory '{directory}' exists")
            except:
                print(f" Directory '{directory}' is missing")
        
        return True
        
    except Exception as e:
        print(f" HDFS connection failed: {e}")
        return False


def check_docker():
    """Check if Docker containers are running"""
    print("\n Checking Docker containers...")
    import subprocess
    
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            containers = [line.split("\t")[0] for line in lines if line]
            
            required_containers = ["minio-server", "hdfs-namenode", "hdfs-datanode"]
            
            for container in required_containers:
                if container in containers:
                    print(f" Container '{container}' is running")
                else:
                    print(f" Container '{container}' is not running")
                    return False
            
            return True
        else:
            print(" Docker command failed")
            return False
            
    except FileNotFoundError:
        print(" Docker is not installed or not in PATH")
        return False
    except Exception as e:
        print(f"Error checking Docker: {e}")
        return False


def main():
    """Run all verification checks"""
    print()
    print("=" * 60)
    print("Infrastructure Verification")
    print("Big Data Project - Weather Impact on Urban Traffic Analysis")
    print("=" * 60)
    print()
    
    docker_ok = check_docker()
    minio_ok = check_minio()
    hdfs_ok = check_hdfs()
    
    print()
    print("=" * 60)
    
    if docker_ok and minio_ok and hdfs_ok:
        print("All systems operational!")
        print()
        print("Access Points:")
        print("  MinIO Console: http://localhost:9001")
        print("  HDFS NameNode: http://localhost:9870")
        print("=" * 60)
        return True
    else:
        print("Some systems are not operational")
        print()
        print("Troubleshooting:")
        if not docker_ok:
            print("  - Start Docker containers: docker-compose up -d")
        if not minio_ok:
            print("  - Run MinIO setup: python scripts/setup_minio.py")
        if not hdfs_ok:
            print("  - Run HDFS setup: python scripts/setup_hdfs.py")
        print("=" * 60)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
