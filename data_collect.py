intake_url = "https://data.austintexas.gov/resource/wter-evkm.json?"
outcome_url = "https://data.austintexas.gov/resource/9t4d-g238.json?"

import os
import requests
import json
import datetime


class DataCollector:
    def __init__(self, limit, url, path_prefix):
        self.position = 0
        self.limit = limit
        self.url = url
        self.path_prefix = path_prefix

        now_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.data_path = f"data/{path_prefix}/{now_time}"

        self.create_data_folder()

    def create_data_folder(self):
        if not os.path.isdir(self.data_path):
            os.makedirs(self.data_path)

    def get_data(self):
        complete_data = []
        while True:
            print(f"Getting {self.path_prefix} data from position: {self.position}")
            complete_url = self.url + f"$limit={self.limit}&$offset={self.position}"

            resp = requests.get(complete_url)
            if resp.status_code == 200:
                data = resp.json()

                if len(data) == 0:
                    break

                complete_data.append(data)

            else:
                print(resp)
                break

            self.position += self.limit

        data_json_path = f"{self.data_path}.json"
        with open(data_json_path, "w") as open_file:
            json.dump(complete_data, open_file)


if __name__ == "__main__":

    collector_intake = DataCollector(limit=1000, url=intake_url, path_prefix="intake")
    collector_intake.get_data()

    collector_outcome = DataCollector(
        limit=1000, url=outcome_url, path_prefix="outcome"
    )
    collector_outcome.get_data()
