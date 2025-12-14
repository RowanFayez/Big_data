"""
Test MinIO - List all buckets and their contents
"""

from minio import Minio

print("=" * 60)
print("Testing MinIO via Ngrok")
print("=" * 60)
print()

# Connect to MinIO locally
client = Minio(
    "localhost:9002",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

# List all buckets
print("Buckets:")
buckets = client.list_buckets()
for bucket in buckets:
    print(f"  - {bucket.name}")
print()

# List contents of each bucket
for bucket in buckets:
    print(f"Contents of '{bucket.name}' bucket:")
    print("-" * 60)
    
    try:
        objects = client.list_objects(bucket.name, recursive=True)
        
        found_files = False
        for obj in objects:
            found_files = True
            size_mb = obj.size / (1024 * 1024)
            print(f"{obj.object_name}")
            print(f"     Size: {size_mb:.2f} MB")
            print(f"     Last Modified: {obj.last_modified}")
            print()
        
        if not found_files:
            print("  (Empty - no files yet)")
            print()
    
    except Exception as e:
        print(f"Error: {e}")
        print()

print("=" * 60)
print("Test Complete!")
print("=" * 60)
