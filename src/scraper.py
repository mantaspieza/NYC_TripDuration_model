import os
import time
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup, ResultSet


class Scraper:
    def __init__(
        self,
        id: str = "CodeAcademy-Final-Project",
        web_browser: str = "Mozilla/5.0",
        general_url_for_monthly_data: str = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        scraping_start_year: str = "2022-01",
        scraping_end_year: str = "2022-01",
        title_of_parquet_file: str = "scraped_yellow_city_taxi_file",
        timeout: int = 2,
    ):
        self.header = {id: web_browser}
        self.url_for_scraping = general_url_for_monthly_data
        self.title_of_parquet_file = title_of_parquet_file
        self.timeout = timeout
        self.start_date = scraping_start_year
        self.end_date = scraping_end_year
        self._datasets_to_download = []

    def get_page_response(self, url: str, header: dict) -> BeautifulSoup:
        self.response = requests.get(url, headers=header)

        soup = BeautifulSoup(self.response.text, "html.parser")
        if not self.response.ok:
            print(f"You've received {self.response} error")
        else:
            return soup

    def collect_all_datasets_to_download(self) -> list:
        soup = self.get_page_response(url=self.url_for_scraping, header=self.header)

        all_downloadable_files = soup.find_all(
            "a", title="Yellow Taxi Trip Records", href=True
        )

        return [dataset["href"] for dataset in all_downloadable_files]

    def generate_dataset_names(self) -> list:
        month_list = pd.period_range(start=self.start_date, end=self.end_date, freq="M")
        
        return [month.strftime("%Y-%m.parquet") for month in month_list if month.strftime("%m") == '12' ]

    def identify_required_datasets(self) -> list:
        all_possible_datasets_to_download = self.collect_all_datasets_to_download()
        required_datasets_names = self.generate_dataset_names()

        for item in all_possible_datasets_to_download:
            for dataset_name in required_datasets_names:
                if dataset_name in item.split("_"):
                    self._datasets_to_download.append(item)
                else:
                    pass

    def download_required_datasets(self, path_to_raw_dataset_folder:str ='../raw_data'):
        # retrieves all files in folder to avoid duplicate download
        files_in_folder = os.listdir(path_to_raw_dataset_folder)
        self.identify_required_datasets()

        for dataset in self._datasets_to_download:
            dataset_name = dataset.split("/")[-1]
            try:
                if dataset_name not in files_in_folder:
                    downloaded_dataset = requests.get(dataset).content

                    with open(f"{path_to_raw_dataset_folder}/{dataset_name}", "wb") as handler:
                        handler.write(downloaded_dataset)
                else:
                    print("dataset alraedy exists")
                    pass
            except Exception as e:
                print(e)
            