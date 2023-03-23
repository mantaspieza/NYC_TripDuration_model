import os

from scraper import Scraper
from transformer import Transformer


class DataPreparation:
    def __init__(
        self,
        data_extraction_start_date: str = "2017-01",
        data_extraction_end_date: str = "2022-01",
        raw_data_path: str = "../raw_data",
        transformed_data_path="../transformed_data/",
    ):
        self.scraper = Scraper(
            scraping_start_year=data_extraction_start_date,
            scraping_end_year=data_extraction_end_date,
        )

        self.raw_data_path = raw_data_path
        self.transformed_data_path = transformed_data_path

    def extract_data(self):
        self.scraper.download_required_datasets(
            path_to_raw_dataset_folder=self.raw_data_path
        )

        for dataset in os.listdir(self.raw_data_path):
            Transformer(
                file_name=dataset, transofmed_data_path=self.transformed_data_path
            ).transform_data()

        print("all transformations completed")
