# ========================================
# STEP 6: Streamlit App (Azure Blob + Parquet Folder Support)
# ========================================
import streamlit as st
import pandas as pd
import time
from azure.storage.blob import BlobServiceClient, ContainerClient
import pyarrow.parquet as pq
import pyarrow as pa
import io
from datetime import datetime

# Azure Blob config
account_name = "trendsdata"
container_name = "trends"
sas_token = "sp=racwdli&st=2025-04-16T19:27:40Z&se=2025-07-26T03:27:40Z&sv=2024-11-04&sr=c&sig=98wbZADF5czK7qOdrCVf2CQ4w7zYghKhYytlsX1N2og%3D"

# BlobService + ContainerClient
blob_service = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=sas_token)
container_client = blob_service.get_container_client(container=container_name)

# Parquet folder path
parquet_folder_prefix = "recommendations/user_recs.parquet/"

st.title("🎬 Real-Time Movie Recommender")

user_id = st.number_input("Select a User ID", min_value=1, step=1)

if st.button("Get My Recommendations"):
    try:
        # List .parquet parts inside the folder
        blob_list = container_client.list_blobs(name_starts_with=parquet_folder_prefix)
        parquet_parts = [blob.name for blob in blob_list if blob.name.endswith(".parquet")]

        # Read all parts
        table_list = []
        for blob_name in parquet_parts:
            blob_client = container_client.get_blob_client(blob_name)
            data = blob_client.download_blob().readall()
            table = pq.read_table(io.BytesIO(data))
            table_list.append(table)

        final_table = pa.concat_tables(table_list)
        df = final_table.to_pandas()

        # Filter for the selected user
        recs = df[df["userId"] == user_id]

        if recs.empty:
            st.warning("No recommendations found.")
        else:
            st.write("🎯 Top Picks:")
            for _, row in recs.iterrows():
                st.write(f"🎥 {row['title']} — Score: {row['score']:.2f}")

    except Exception as e:
        st.error(f"❌ Failed to read recommendations: {str(e)}")

# Submit new rating (saved to DBFS)
st.subheader("📝 Submit a new rating")
movie_id = st.number_input("Movie ID", min_value=1)
rating = st.slider("Rating", min_value=0.5, max_value=5.0, step=0.5)

if st.button("Submit Rating"):
    try:
        # Format timestamp like: 2005-04-02 23:53:47
        formatted_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Create the new row
        new_rating = pd.DataFrame([[user_id, movie_id, rating, formatted_timestamp]],
                                  columns=["userId", "movieId", "rating", "timestamp"])

        # Convert DataFrame to CSV bytes
        csv_buffer = io.StringIO()
        new_rating.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode()

        # Save to Azure Blob
        blob_name = f"ratings_stream/rating_{int(time.time())}.csv"
        blob_client = container_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(csv_bytes, overwrite=True)

        st.success("✅ Rating submitted and saved to blob storage!")

    except Exception as e:
        st.error(f"❌ Failed to submit rating: {str(e)}")