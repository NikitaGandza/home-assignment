import os
import dotenv
import psycopg2
import pandas as pd

dotenv.load_dotenv()


class HomeAssignment:
    def __init__(self):
        self.hello = "Hello, Jeff-App!"


class Exporter(HomeAssignment):
    def __init__(self):
        super().__init__()
        self.db_user = os.environ.get("db_user")
        self.db_password = os.environ.get("db_password")
        self.db_database = os.environ.get("db_database")
        self.db_host = os.environ.get("db_host")
        self.db_port = os.environ.get("db_port")

        self.con = psycopg2.connect(
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_database
        )
        self.cur = self.con.cursor()

    def execute_query(self, query):
        self.cur.execute(query)
        rows = self.cur.fetchall()
        self.cur.close()
        self.con.commit()
        return rows


class Processor(HomeAssignment):
    def __init__(self):
        super().__init__()
        self.raw_columns = ["id", "created", "lead_id", "algorithm", "country_code",
                            "partner_id_ranking", "no_valid_offers"]
        self.processed_columns = ["id", "created", "lead_id", "algorithm", "country_code",
                                  "partner_id_ranking", "partner_position", "no_valid_offers"]
        self.raw_file_path = "raw_df.csv"
        self.processed_file_path = "processed_df.csv"

    def create_df(self, data):
        df = pd.DataFrame(data, columns=self.raw_columns)
        df.to_csv(self.raw_file_path, index=False)
        return df

    def preprocess(self, df):
        df["partner_id_ranking"] = df["partner_id_ranking"].str.replace("{", "").str.replace("}", "")
        df["partner_id_ranking"] = df["partner_id_ranking"].str.split(",")
        
        return df

    def explode_df(self, df):
        df_explode = df.explode("partner_id_ranking", ignore_index=True)
        df_explode['partner_position'] = df_explode.groupby(['id']).cumcount() + 1
        df_explode = df_explode[self.processed_columns]
        df_explode.to_csv(self.processed_file_path, index=False)
        return df_explode


def _export_data():
    exporter = Exporter()
    query = """
        SELECT * FROM offer_listings WHERE no_valid_offers = False
        """
    raw_data = exporter.execute_query(query)
    return raw_data


def main():
    raw_data = _export_data()
    process_data = Processor()
    df = process_data.create_df(raw_data)
    print(f"Total raw rows: {len(df.index)}")
    df = process_data.preprocess(df)
    df = process_data.explode_df(df)
    print(f"Total processed rows: {len(df.index)}")


if __name__ == '__main__':
    main()
