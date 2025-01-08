import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, String, Engine
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class DatabaseConnection:
    """Database connection handler class"""

    @staticmethod
    def create_mysql_engine() -> Engine:
        return create_engine(
            f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}"
            f"@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
        )

    @staticmethod
    def create_postgres_engine() -> Engine:
        return create_engine(
            f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
            f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
        )


class DataProcessor:
    """Data processing class"""

    def __init__(self):
        self.mysql_engine = DatabaseConnection.create_mysql_engine()
        self.postgres_engine = DatabaseConnection.create_postgres_engine()
        self.column_dtypes = {
            "client_id": Integer,
            "name": String(100),
            "name_user": String(100),
            "month": String(10),
            "message_count": Integer,
        }

    def extract_data(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Extract data from MySQL database"""
        try:
            logger.info("Extracting data from MySQL...")
            return (
                pd.read_sql_table("WAMessages", con=self.mysql_engine),
                pd.read_sql_table("Client", con=self.mysql_engine),
                pd.read_sql_table("User", con=self.mysql_engine),
            )
        except SQLAlchemyError as e:
            logger.error(f"Database extraction error: {str(e)}")
            raise

    def transform_data(
        self, df_messages: pd.DataFrame, df_client: pd.DataFrame, df_user: pd.DataFrame
    ) -> pd.DataFrame:
        """Transform the extracted data"""
        try:
            logger.info("Transforming data...")

            # Process messages data
            df_messages = df_messages.assign(
                created_at=pd.to_datetime(df_messages["created_at"]),
                month=lambda x: x["created_at"].dt.to_period("M"),
            )

            # Get messages per month
            df_messages_per_month = (
                df_messages[df_messages["status"].isin(["delivered", "read"])]
                .groupby(["client_id", "user_id", "month"], as_index=False)
                .size()
                .rename(columns={"size": "message_count"})
            )

            # Merge dataframes
            df_merged = df_messages_per_month.merge(
                df_client[["id", "name"]],
                left_on="client_id",
                right_on="id",
                how="left",
            ).merge(
                df_user[["id", "name"]],
                left_on="user_id",
                right_on="id",
                how="left",
                suffixes=("", "_user"),
            )

            return df_merged[
                ["client_id", "name", "name_user", "month", "message_count"]
            ]
        except Exception as e:
            logger.error(f"Data transformation error: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame) -> None:
        """Load data to PostgreSQL and CSV files"""
        try:
            logger.info("Loading data to PostgreSQL...")

            # Prepare data for loading
            df = df.assign(
                month=lambda x: x["month"].astype(str),
                message_count=lambda x: x["message_count"].astype(int),
            )

            # Load to PostgreSQL
            df.to_sql(
                name="WAMessagesSummary",
                con=self.postgres_engine,
                if_exists="replace",
                index=False,
                dtype=self.column_dtypes,
            )

            # Prepare and save CSV files
            previous_month = (
                datetime.now().replace(day=1) - timedelta(days=1)
            ).strftime("%Y-%m")

            final_result = df[df["month"] == previous_month].rename(
                columns={
                    "name": "Nama Perusahaan",
                    "name_user": "Nama Divisi",
                    "month": "Periode",
                    "message_count": "Jumlah Pesan Terkirim",
                }
            )[["Nama Perusahaan", "Nama Divisi", "Periode", "Jumlah Pesan Terkirim"]]

            # Save CSV files
            output_dir = os.path.join("output", previous_month)
            os.makedirs(output_dir, exist_ok=True)

            for client_name, group in final_result.groupby("Nama Perusahaan"):
                output_path = os.path.join(output_dir, f"{client_name}.csv")
                group.to_csv(output_path, index=False)

            logger.info("Data loading completed successfully")
        except Exception as e:
            logger.error(f"Data loading error: {str(e)}")
            raise


def main():
    """Main execution function"""
    try:
        processor = DataProcessor()
        df_messages, df_client, df_user = processor.extract_data()
        transformed_data = processor.transform_data(df_messages, df_client, df_user)
        processor.load_data(transformed_data)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
