import os
import numpy as np
import pandas as pd

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

pd.options.display.float_format = "{:.3f}".format


class Transformer:
    """
    Transformer class which takes in raw dataset, removes outliers and non required columns providing transformed dataset.
    """

    def __init__(
        self,
        raw_file_location: str = "./raw_data/",
        period_start_day: str = 24,
        period_end_day: str = 26,
        file_name: str = "yellow_tripdata_2021-12.parquet",
        transofmed_data_path="./transformed_data/",
    ):
        """
        Initialization function for Transformer class.

        Args:
            raw_file_location (str, optional): Location where raw datasets are stored. Defaults to "../raw_data/".
            period_start_day (str, optional): Start date to define date for dataset filtering. Defaults to 24.
            period_end_day (str, optional): End date to define date for dataset filtering. Defaults to 26.
            file_name (str, optional): File name used to identify dataset. Defaults to "yellow_tripdata_2021-12.parquet".
            transofmed_data_path (str, optional): System path where to save transformed datasets. Defaults to "../transformed_data/".
        """

        self.period_start_day = period_start_day
        self.period_end_day = period_end_day
        self.file_name = file_name
        self.dataframe = pd.read_parquet(f"{raw_file_location}{file_name}")
        self.transformed_data_path = transofmed_data_path

    def get_start_end_datetime(self, dataframe_name: str) -> None:
        """
        Identifies Start and end dates for Pickup and dropoff datetime columns according to predefined rules and saves them as class variables.

        Args:
            dataframe_name (str): File name used to identify dataset.
        """
        year_month = dataframe_name.split("_")[-1].split(".")[0]

        self.start_date = dt.strptime(year_month, "%Y-%m") + relativedelta(
            days=self.period_start_day - 1
        )
        self.end_date = self.start_date + relativedelta(days=3)
        self.end_date_dropoff = self.start_date + relativedelta(days=3, hours=1)

    def select_christmas_period(self) -> None:
        """
        Performs daframe filtering using defined start and end dates for Pickup and dropoff datetime columns.
        Overwrites dataset defined in class initiation.
        """
        self.get_start_end_datetime(dataframe_name=self.file_name)

        self.dataframe = self.dataframe[
            (self.dataframe["tpep_pickup_datetime"] >= self.start_date)
            & (self.dataframe["tpep_pickup_datetime"] < self.end_date)
        ]

        self.dataframe = self.dataframe[
            (self.dataframe["tpep_dropoff_datetime"] >= self.start_date)
            & (self.dataframe["tpep_dropoff_datetime"] < self.end_date_dropoff)
        ]

    def restrict_vendor_id(self) -> None:
        """
        Removes outliers from VendorID column.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (self.dataframe["VendorID"] == 1) | (self.dataframe["VendorID"] == 2)
        ]

    def create_trip_duration_column(self) -> None:
        """
        Creates trip duration column, which is used as a target variable in model creation.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe["trip_duration_minutes"] = (
            (
                self.dataframe["tpep_dropoff_datetime"]
                - self.dataframe["tpep_pickup_datetime"]
            ).dt.total_seconds()
        ) / 60

    def restrict_passenger_count(self, maximum_number_of_passengers: int = 6) -> None:
        """
        Removes outliers from passenger count column.
        Overwrites dataset defined in class initiation.

        Args:
            maximum_number_of_passengers (int, optional): Number of maximum passengers in taxi. Defaults to 6.

        """
        passenger_count_mode_value = self.dataframe["passenger_count"].mode()
        self.dataframe["passenger_count"].fillna(
            passenger_count_mode_value, inplace=True
        )
        self.dataframe = self.dataframe[
            self.dataframe["passenger_count"] <= maximum_number_of_passengers
        ]

    def restrict_ratecodeid(
        self, ratecode_1: int = 1, ratecode_2: int = 2, ratecode_3: int = 3
    ) -> None:
        """
        Fills in missing values using most freaquent (mode) value of the Rate Code ID column and removes outliers.
        Overwrites dataset defined in class initiation.

        Args:
            ratecode_1 (int, optional): First ratecodeid. Defaults to 1.
            ratecode_2 (int, optional): Second ratecodeid. Defaults to 2.
            ratecode_3 (int, optional): Third ratecodeid. Defaults to 3.
        """
        ratecode_id_mode = self.dataframe["RatecodeID"].mode()
        self.dataframe["RatecodeID"].fillna(ratecode_id_mode, inplace=True)

        self.dataframe = self.dataframe[
            (self.dataframe["RatecodeID"] == ratecode_1)
            | (self.dataframe["RatecodeID"] == ratecode_2)
            | (self.dataframe["RatecodeID"] == ratecode_3)
        ]

    def restrict_store_fwd_flag(self) -> None:
        """
        Fills in missing values in store and forward flag column, assuming with yes (Y) value.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe["store_and_fwd_flag"].fillna("Y", inplace=True)

    def restrict_payment_type(self) -> None:
        """
        Restricts payment type to cash (2) or credit_card (1).
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (self.dataframe["payment_type"] == 1)
            | (self.dataframe["payment_type"] == 2)
        ]

    def filter_pickup_locations(self) -> None:
        """
        Filters out unknown pickup locations.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (self.dataframe["PULocationID"] != 264)
            | (self.dataframe["PULocationID"] != 265)
        ]

    def filter_pickup_location_outliers(self) -> None:
        """
        Filters out ouliers.
        Overwrites dataset defined in class initiation.
        """
        temp_pickup = (
            self.dataframe["PULocationID"].value_counts().reset_index(name="count")
        )
        pickup_location_outliers = list(
            temp_pickup[temp_pickup["count"] <= 30]["index"]
        )

        for location in pickup_location_outliers:
            self.dataframe = self.dataframe[self.dataframe["PULocationID"] != location]
            self.dataframe = self.dataframe[self.dataframe["DOLocationID"] != location]

    def filter_dropoff_location_outliers(self) -> None:
        """
        Filters out outliers.
        Overwrites dataset defined in class initiation.
        """
        temp_dropoff = (
            self.dataframe["DOLocationID"].value_counts().reset_index(name="count")
        )
        dropoff_location_outliers = list(
            temp_dropoff[temp_dropoff["count"] <= 30]["index"]
        )

        for location in dropoff_location_outliers:
            self.dataframe = self.dataframe[self.dataframe["PULocationID"] != location]
            self.dataframe = self.dataframe[self.dataframe["DOLocationID"] != location]

    def filter_dropoff_locations(self) -> None:
        """
        Filters out unknown drop off locations.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (self.dataframe["DOLocationID"] != 264)
            | (self.dataframe["DOLocationID"] != 265)
        ]

    def filter_trip_duration_outliers(self) -> None:
        """
        Filters out trip duration outliers that are less or equal to 0, and greateror or equal than 99.5 percentile.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (
                self.dataframe["trip_duration_minutes"]
                <= np.quantile(self.dataframe["trip_duration_minutes"], 0.995)
            )
            & (self.dataframe["trip_duration_minutes"] > 0)
        ]

    def filter_trip_distance_outliers(self) -> None:
        """
        Filtesr out trip distance outliers that are less or equal to 0, and greater  than 99.5 percentile.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (
                self.dataframe["trip_distance"]
                < np.quantile(self.dataframe["trip_distance"], 0.995)
            )
            & (self.dataframe["trip_distance"] > 0)
        ]

    def filter_fare_amount_outliers(self) -> None:
        """
        Filters out fare amount outliers from dataset.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            (self.dataframe["fare_amount"] > 0)
            & (
                self.dataframe["fare_amount"]
                <= np.quantile(self.dataframe["fare_amount"], 0.99)
            )
        ]

    def filter_tip_amount_outliers(self) -> None:
        """
        Filters out tip amount outliers.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe = self.dataframe[
            self.dataframe["tip_amount"]
            <= np.quantile(self.dataframe["tip_amount"], 0.999)
        ]

    def filter_tolls_amount_outliers(self) -> None:
        """
        Filters out tolls amount outliers.
        Overwrites dataset defined in class initiation.
        """
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
    ) -> None:
        """
        Removes outlier columns from dataset.
        Overwrites dataset defined in class initiation.

        Args:
            columns_list (list, optional): List of columns to be removed from dataset. Defaults to [ "fare_amount", "tip_amount", "total_amount", "extra", "mta_tax", "improvement_surcharge", "airport_fee", "congestion_surcharge", ].
        """
        self.dataframe.drop(columns_list, axis=1, inplace=True)

    def create_weekend_column(self) -> None:
        """
        Creates boolean weekend feature identifying if the pickup takes place on weekend.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe["is_weekend"] = (
            self.dataframe["tpep_pickup_datetime"].dt.weekday >= 5
        )

    def create_weekday_column(self) -> None:
        """
        Creates weekday column identifying day of the week of the pickup date.
        Overwrites dataset defined in class initiation.
        """
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

    def define_if_business_hours(self, value: int) -> bool:
        """
        Defines if pickup time is within business hours.

        Args:
            value (int): Hour of the pickup time

        Returns:
            bool: True if in within business hours
        """
        if value >= 7 and value <= 18:
            return True
        else:
            return False

    def define_time_of_day(self, hour: int) -> str:
        """
        Identifies time of the day.

        Args:
            hour (int): Hour of the pickup

        Returns:
            str: Time of day: Morning, Afternoon, Evening, Night
        """
        if hour >= 5 and hour < 12:
            return "Morning"
        if hour >= 12 and hour < 17:
            return "Afternoon"
        if hour >= 17 and hour < 21:
            return "Evening"
        if 0 <= hour < 5 or 21 <= hour < 25:
            return "Night"

    def create_hour_column(self) -> None:
        """
        Extracts hour from pickup datetime column and saves it as new column.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe["hour"] = self.dataframe["tpep_pickup_datetime"].dt.hour

    def create_business_hour_column(self) -> None:
        """
        Creates new column identifying if pickup took place within business hours.
        Overwrites dataset defined in class initiation.
        """

        self.dataframe["is_business_hours"] = self.dataframe["hour"].apply(
            self.define_if_business_hours
        )

    def create_time_of_day_column(self) -> None:
        """
        Creates time of day column using "define_time_of_day" function.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe["time_of_day"] = self.dataframe["hour"].apply(
            self.define_time_of_day
        )

    def add_year_column(self) -> None:
        """
        Extracts year from pickup datetime column as new column.
        Overwrites dataset defined in class initiation.
        """
        self.dataframe["year"] = self.dataframe["tpep_pickup_datetime"].dt.year

    def remove_time_columns(
        self,
        cols_to_remove: list = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "hour",
        ],
    ) -> None:
        """
        Removes date-time related columns from dataset.

        Args:
            cols_to_remove (list, optional): List of columns which were used in date-time feature engineering. Defaults to [ "tpep_pickup_datetime", "tpep_dropoff_datetime", "hour", ].
        """
        self.dataframe.drop(cols_to_remove, axis=1, inplace=True)

    def prepare_dataset(self) -> None:
        """
        Function used to orchestrate all the transormation steps.
        """

        self.select_christmas_period()

        # categorical columns
        self.restrict_vendor_id()
        self.create_trip_duration_column()
        self.restrict_passenger_count()
        self.restrict_ratecodeid()
        self.restrict_store_fwd_flag()
        self.restrict_payment_type()
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
        """
        Main function used to read in dataset, perfrom transformations and save the transformed dataframe.

        """
        self.prepare_dataset()
        if "transformed_data" not in os.listdir("./"):
            print("transformed data folder is created")
            os.mkdir("./transformed_data")
        self.dataframe.to_parquet(
            f"{self.transformed_data_path}transformed_{self.file_name.split('_')[-1]}"
        )
