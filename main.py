import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, String

# Load environment variables from .env file
load_dotenv()


def mysql_engine():
    return create_engine(
        f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
    )


def postgres_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )


if __name__ == "__main__":
    # EXTRACT DATA
    # Query from MySQL database
    df_messages = pd.read_sql_table("WAMessages", con=mysql_engine())
    df_client = pd.read_sql_table("Client", con=mysql_engine())
    df_user = pd.read_sql_table("User", con=mysql_engine())

    # TRANSFORM DATA
    # Convert created_at to datetime
    df_messages = df_messages.assign(
        created_at=lambda x: pd.to_datetime(x["created_at"]),
        month=lambda x: x["created_at"].dt.to_period("M"),
    )

    # Count messages per month when status in ["delivered", "read"]
    df_messages_per_month = (
        df_messages[df_messages["status"].isin(["delivered", "read"])]
        .groupby(["client_id", "user_id", "month"])
        .size()
        .reset_index(name="message_count")
    )

    # Join table WAMessages, Client, and User
    df_merged = pd.merge(
        df_messages_per_month,
        df_client,
        how="left",
        left_on="client_id",
        right_on="id",
        suffixes=("", "_client"),
    ).merge(
        df_user,
        how="left",
        left_on="user_id",
        right_on="id",
        suffixes=("", "_user"),
    )

    # Create a summary DataFrame
    df_summary = df_merged[
        ["client_id", "name", "name_user", "month", "message_count"]
    ].copy()

    # Convert Period to varchar(10) format and ensure message_count is integer
    df_summary = df_summary.assign(
        month=lambda x: x["month"].astype(str),
        message_count=lambda x: x["message_count"].astype(int),
    )

    # Save to PostgreSQL
    df_summary.to_sql(
        name="WAMessagesSummary",
        con=postgres_engine(),
        if_exists="append",
        index=False,
        dtype={
            "client_id": Integer,
            "name": String(100),
            "name_user": String(100),
            "month": String(10),
            "message_count": Integer,
        },
    )

    # Calculate the previous month
    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    previous_month = last_day_of_previous_month.strftime("%Y-%m")

    # Filter and rename columns for final result
    final_result = (
        df_summary[df_summary["month"] == previous_month]
        .rename(
            columns={
                "name": "Nama Perusahaan",
                "name_user": "Nama Divisi",
                "month": "Periode",
                "message_count": "Jumlah Pesan Terkirim",
            }
        )[["Nama Perusahaan", "Nama Divisi", "Periode", "Jumlah Pesan Terkirim"]]
        .copy()
    )

    # Create output directory for the month
    output_dir = os.path.join("output", previous_month)
    os.makedirs(output_dir, exist_ok=True)

    # Save CSV files for each company
    for client_name in final_result["Nama Perusahaan"].unique():
        client_df = final_result[final_result["Nama Perusahaan"] == client_name].copy()
        output_path = os.path.join(output_dir, f"{client_name}.csv")
        client_df.to_csv(output_path, index=False)
