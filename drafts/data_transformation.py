import os, glob
from pyspark.sql import SparkSession


class DataTransformer:
    def __init__(self):
        self.spark = None
        self.df_intake = None
        self.df_outcome = None

    def get_latest_directory_on_folder(self, folder):
        return max(glob.glob(os.path.join(folder, "*/")), key=os.path.getmtime)

    def read_all_jsons_on_folder(self, intake_folder, outcome_folder):
        self.spark = SparkSession.builder.appName("ShelterTransform").getOrCreate()

        latest_intake = self.get_latest_directory_on_folder(intake_folder)
        print(f"Latest intake folder created:{latest_intake}")

        latest_outcome = self.get_latest_directory_on_folder(outcome_folder)
        print(f"Latest outcome folder created:{latest_intake}")

        self.df_intake = self.spark.read.json(f"{latest_intake}/*.json")
        self.df_outcome = self.spark.read.json(f"{latest_outcome}/*.json")

    def display_input_data_schema(self):
        self.df_intake.printSchema()
        self.df_outcome.printSchema()

    def join_animal_data(self):
        self.df_intake.createOrReplaceTempView("intake")
        self.df_outcome.createOrReplaceTempView("outcome")

        self.joint_df = self.spark.sql(
            """
            SELECT *
            FROM intake
            LEFT JOIN outcome ON intake.animal_id = outcome.animal_id
            LIMIT 2

        """
        ).show()


df = DataTransformer()

df.read_all_jsons_on_folder("data/intake/", "data/outcome/")
df.join_animal_data()
