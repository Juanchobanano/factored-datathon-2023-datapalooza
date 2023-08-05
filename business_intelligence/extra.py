import delta_sharing
from pprint import pprint

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "./config.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
tables = client.list_all_tables()

pprint(tables)
