"""
Test Ngrok MinIO Connection
Run this to verify your team can access MinIO via ngrok
"""

from minio import Minio
from minio.error import S3Error

# Your ngrok URL
NGROK_URL = "unwakened-gewgawed-connie.ngrok-free.dev"

print("=" * 60)
print("Testing MinIO Connection via Ngrok")
print("=" * 60)
print()
print(f"Ngrok URL: {NGROK_URL}")
print(f"Username: minioadmin")
print(f"Password: minioadmin123")
print()

try:
    # Initialize MinIO client
    print("🔗 Connecting to MinIO via ngrok...")
    client = Minio(
        NGROK_URL,
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=True  # HTTPS for ngrok
    )
    
    # Test 1: List buckets
    print("Connection successful!")
    print()
    print("Listing buckets...")
    buckets = client.list_buckets()
    
    if buckets:
        print(f"Found {len(buckets)} bucket(s):")
        for bucket in buckets:
            print(f"  - {bucket.name}")
    else:
        print("  No buckets found. Please create bronze, silver, gold buckets first.")
    
    print()
    print("=" * 60)
    print("SUCCESS! Your team can access MinIO via ngrok!")
    print("=" * 60)
    print()
    print("Share this URL with your team:")
    print(f"  {NGROK_URL}")
    print()
    print("They should use this code:")
    print(f'''
from minio import Minio

client = Minio(
    "{NGROK_URL}",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=True
)

# List buckets
buckets = client.list_buckets()
for bucket in buckets:
    print(bucket.name)
''')
    
except S3Error as e:
    print(f"MinIO Error: {e}")
    print()
    print("Possible issues:")
    print("  1. MinIO container not running")
    print("  2. Ngrok tunnel not active")
    print("  3. Wrong credentials")
    
except Exception as e:
    print(f"Connection Error: {e}")
    print()
    print("Possible issues:")
    print("  1. Ngrok tunnel not running")
    print("  2. Network/firewall blocking connection")
    print("  3. Invalid ngrok URL")
    print()
    print("Make sure ngrok is running:")
    print("  .\\ngrok.exe http 9002")
