import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "./config.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
tables = client.list_all_tables()

print(tables)

table_url = profile_file + "#share__products_bi.platinum.reviews_count_per_day"

data = delta_sharing.load_as_pandas(table_url)

print(data)