# %%

import os, glob
from pyspark.sql import SparkSession


def get_Latest_directory_on_folder(folder):
    return max(glob.glob(os.path.join(folder, "*/")), key=os.path.getmtime)


spark = SparkSession.builder.appName("ShelterJSON").getOrCreate()

root_path = "data/"
dir_path = "path/to/json/files/*.json"


latest_intake = get_Latest_directory_on_folder("data/intake/")
print()
print(get_Latest_directory_on_folder("data/outcome/"))

df = spark.read.json(f"{latest_intake}/*.json")

df.show(10)
