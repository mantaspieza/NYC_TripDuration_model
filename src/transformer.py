import os
import numpy as np
import pandas as pd

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

pd.options.display.float_format = "{:.3f}".format


class Transformer:
    def __init__(
        self,
        raw_file_location: str = "../raw_data/",
        period_start_day: str = 24,
        period_end_day: str = 26,
        file_name: str = "yellow_tripdata_2021-12.parquet",
        transofmed_data_path="../transformed_data/",
    ):
        # self._raw_file_location = raw_file_location
        self.period_start_day = period_start_day
        self.period_end_day = period_end_day
        self.file_name = file_name
        self.dataframe = pd.read_parquet(f"{raw_file_location}{file_name}")
        self.transformed_data_path = transofmed_data_path

        # init metu paduot pavadinima .parquet failo, ir init metu pd.read_parquet

    # def read_dataframe(self, dataframe_name: str) -> pd.DataFrame:
    #     df = pd.read_parquet(f"{self._raw_file_location}{dataframe_name}")
    #     return df

    def get_start_end_datetime(self, dataframe_name: str):
        year_month = dataframe_name.split("_")[-1].split(".")[0]

        self.start_date = dt.strptime(year_month, "%Y-%m") + relativedelta(
            days=self.period_start_day - 1
        )
        self.end_date = self.start_date + relativedelta(days=3)
        self.end_date_dropoff = self.start_date + relativedelta(days=3, hours=1)

    def select_christmas_period(self) -> pd.DataFrame:
        self.get_start_end_datetime(dataframe_name=self.file_name)

        self.dataframe = self.dataframe[
            (self.dataframe["tpep_pickup_datetime"] >= self.start_date)
            & (self.dataframe["tpep_pickup_datetime"] < self.end_date)
        ]

        self.dataframe = self.dataframe[
            (self.dataframe["tpep_dropoff_datetime"] >= self.start_date)
            & (self.dataframe["tpep_dropoff_datetime"] < self.end_date_dropoff)
        ]

    def restrict_vendor_id(self) -> pd.DataFrame:
        self.dataframe = self.dataframe[
            (self.dataframe["VendorID"] == 1) | (self.dataframe["VendorID"] == 2)
        ]

    def create_trip_duration_column(self) -> pd.DataFrame:
        self.dataframe["trip_duration_minutes"] = (
            (
                self.dataframe["tpep_dropoff_datetime"]
                - self.dataframe["tpep_pickup_datetime"]
            ).dt.total_seconds()
        ) / 60

    def restrict_passenger_count(
        self, maximum_number_of_passengers: int = 6
    ) -> pd.DataFrame:
        passenger_count_mode_value = self.dataframe["passenger_count"].mode()
        self.dataframe["passenger_count"].fillna(
            passenger_count_mode_value, inplace=True
        )
        self.dataframe = self.dataframe[
            self.dataframe["passenger_count"] <= maximum_number_of_passengers
        ]

    def restrict_ratecodeid(
        self, ratecode_1: int = 1, ratecode_2: int = 2, ratecode_3: int = 3
    ):
        ratecode_id_mode = self.dataframe["RatecodeID"].mode()
        self.dataframe["RatecodeID"].fillna(ratecode_id_mode, inplace=True)

        self.dataframe = self.dataframe[
            (self.dataframe["RatecodeID"] == ratecode_1)
            | (self.dataframe["RatecodeID"] == ratecode_2)
            | (self.dataframe["RatecodeID"] == ratecode_3)
        ]

    def restrict_store_fwd_flag(self):
        self.dataframe["store_and_fwd_flag"].fillna("Y", inplace=True)

    def restrict_payment_type(self):
        self.dataframe = self.dataframe[
            (self.dataframe["payment_type"] == 1)
            | (self.dataframe["payment_type"] == 2)
        ]

    # def transform_congestion_surcharge(self):
    #     if self.file_name != 'yellow_tripdata_2018-12.parquet':
    #         self.dataframe = self.dataframe[
    #             (self.dataframe["congestion_surcharge"] == 0)
    #             | (self.dataframe["congestion_surcharge"] == 2.5)
    #         ]
    #         self.dataframe["congestion_surcharge"] = self.dataframe[
    #             "congestion_surcharge"
    #         ].replace(2.5, 1)
    #     else:
    #         self.dataframe['congestion_surcharge'].fillna(0,inplace=True)

    # def transform_airport_fee(self):
    #     self.dataframe["airport_fee"].fillna(0, inplace=True)
    #     self.dataframe = self.dataframe[
    #         (self.dataframe["airport_fee"] == 0)
    #         | (self.dataframe["airport_fee"] == 1.25)
    # ]
    # self.dataframe["airport_fee"] = self.dataframe["airport_fee"].replace(1.25, 1)

    def filter_pickup_locations(self):
        self.dataframe = self.dataframe[
            (self.dataframe["PULocationID"] != 264)
            | (self.dataframe["PULocationID"] != 265)
        ]

    def filter_pickup_location_outliers(self):
        temp_pickup = (
            self.dataframe["PULocationID"].value_counts().reset_index(name="count")
        )
        pickup_location_outliers = list(
            temp_pickup[temp_pickup["count"] <= 30]["index"]
        )

        for location in pickup_location_outliers:
            self.dataframe = self.dataframe[self.dataframe["PULocationID"] != location]
            self.dataframe = self.dataframe[self.dataframe["DOLocationID"] != location]

    def filter_dropoff_location_outliers(self):
        temp_dropoff = (
            self.dataframe["DOLocationID"].value_counts().reset_index(name="count")
        )
        dropoff_location_outliers = list(
            temp_dropoff[temp_dropoff["count"] <= 30]["index"]
        )

        for location in dropoff_location_outliers:
            self.dataframe = self.dataframe[self.dataframe["PULocationID"] != location]
            self.dataframe = self.dataframe[self.dataframe["DOLocationID"] != location]

    def filter_dropoff_locations(self):
        self.dataframe = self.dataframe[
            (self.dataframe["DOLocationID"] != 264)
            | (self.dataframe["DOLocationID"] != 265)
        ]

    def filter_trip_duration_outliers(self):
        self.dataframe = self.dataframe[
            (
                self.dataframe["trip_duration_minutes"]
                <= np.quantile(self.dataframe["trip_duration_minutes"], 0.995)
            )
            & (self.dataframe["trip_duration_minutes"] > 0)
        ]

    def filter_trip_distance_outliers(self):
        self.dataframe = self.dataframe[
            (
                self.dataframe["trip_distance"]
                < np.quantile(self.dataframe["trip_distance"], 0.995)
            )
            & (self.dataframe["trip_distance"] > 0)
        ]

    def filter_fare_amount_outliers(self):
        self.dataframe = self.dataframe[
            (self.dataframe["fare_amount"] > 0)
            & (
                self.dataframe["fare_amount"]
                <= np.quantile(self.dataframe["fare_amount"], 0.99)
            )
        ]

    def filter_tip_amount_outliers(self):
        self.dataframe = self.dataframe[
            self.dataframe["tip_amount"]
            <= np.quantile(self.dataframe["tip_amount"], 0.999)
        ]

    def filter_tolls_amount_outliers(self):
        self.dataframe = self.dataframe[
            self.dataframe["tolls_amount"]
            <= np.quantile(self.dataframe["tolls_amount"], 0.99)
        ]

        self.dataframe.loc[self.dataframe.tolls_amount > 0, "tolls_amount"] = 1

    def remove_outlier_columns(
        self,
        columns_list: list = [
            "fare_amount",
            "tip_amount",
            "total_amount",
            "extra",
            "mta_tax",
            "improvement_surcharge",
            "airport_fee",
            "congestion_surcharge",
        ],
    ):
        self.dataframe.drop(columns_list, axis=1, inplace=True)

    def create_weekend_column(self):
        self.dataframe["is_weekend"] = (
            self.dataframe["tpep_pickup_datetime"].dt.weekday >= 5
        )

    def create_weekday_column(self):
        self.dataframe["weekday"] = self.dataframe["tpep_pickup_datetime"].dt.weekday
        self.dataframe["weekday"] = self.dataframe["weekday"].map(
            {
                0: "Monday",
                1: "Tuesday",
                2: "Wednesday",
                3: "Thursday",
                4: "Friday",
                5: "Saturday",
                6: "Sunday",
            }
        )

    def define_if_business_hours(self, value):
        if value >= 7 and value <= 18:
            return True
        else:
            return False

    def define_time_of_day(self, hour):
        if hour >= 5 and hour < 12:
            return "Morning"
        if hour >= 12 and hour < 17:
            return "Afternoon"
        if hour >= 17 and hour < 21:
            return "Evening"
        if 0 <= hour < 5 or 21 <= hour < 25:
            return "Night"

    def create_hour_column(self):
        self.dataframe["hour"] = self.dataframe["tpep_pickup_datetime"].dt.hour

    def create_business_hour_column(self):
        self.dataframe["is_business_hours"] = self.dataframe["hour"].apply(
            self.define_if_business_hours
        )

    def create_time_of_day_column(self):
        self.dataframe["time_of_day"] = self.dataframe["hour"].apply(
            self.define_time_of_day
        )

    def add_year_column(self):
        self.dataframe["year"] = self.dataframe["tpep_pickup_datetime"].dt.year

    def remove_time_columns(
        self,
        cols_to_remove: list = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "hour",
        ],
    ):
        self.dataframe.drop(cols_to_remove, axis=1, inplace=True)

    def prepare_dataset(self) -> pd.DataFrame:
        # self.get_start_end_datetime(dataframe_name=self.file_name)

        self.select_christmas_period()  ####

        # categorical columns
        self.restrict_vendor_id()  #####
        self.create_trip_duration_column()
        self.restrict_passenger_count()
        self.restrict_ratecodeid()
        self.restrict_store_fwd_flag()
        self.restrict_payment_type()
        # self.transform_congestion_surcharge()
        # self.transform_airport_fee()
        self.filter_pickup_locations()
        self.filter_pickup_location_outliers()
        self.filter_dropoff_location_outliers()
        self.filter_dropoff_locations()

        # continuous columns
        self.filter_trip_duration_outliers()
        self.filter_trip_distance_outliers()
        self.filter_fare_amount_outliers()
        self.filter_tip_amount_outliers()
        self.filter_tolls_amount_outliers()
        ### should i add total amount oulier filtering ?

        # removing outlier columns
        self.remove_outlier_columns()

        # adding custom features
        self.create_weekend_column()
        self.create_weekday_column()
        self.create_hour_column()
        self.create_business_hour_column()
        self.create_time_of_day_column()
        self.add_year_column()

        # removing time related columns
        self.remove_time_columns()

    def transform_data(self) -> None:
        self.prepare_dataset()

        self.dataframe.to_parquet(
            f"{self.transformed_data_path}transformed_{self.file_name.split('_')[-1]}"
        )
