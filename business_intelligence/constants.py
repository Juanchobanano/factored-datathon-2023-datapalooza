import streamlit as st
import delta_sharing
import json
from os.path import exists

# Get Delta Sharing Client.
config_share = {
    "shareCredentialsVersion": st.secrets.delta_sharing_credentials.shareCredentialsVersion,
    "bearerToken": st.secrets.delta_sharing_credentials.bearerToken,
    "endpoint": st.secrets.delta_sharing_credentials.endpoint,
    "expirationTime": st.secrets.delta_sharing_credentials.expirationTime
}
profile_file = "./config.share"
if not exists(profile_file):
    with open(profile_file, "w") as f:
        f.write(json.dumps(config_share))

client = delta_sharing.SharingClient(profile_file)