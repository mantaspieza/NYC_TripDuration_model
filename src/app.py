import pandas as pd
import numpy as np
import pickle as pkl
import uvicorn

from fastapi import FastAPI, Request
from pydantic import BaseModel


class Health(BaseModel):
    status: int


class PredictionInput(BaseModel):
    VendorID: int
    passenger_count: int
    trip_distance: float
    RatecodeID: int
    store_and_fwd_flag: str
    PULocationID: int
    DOLocationID: int
    payment_type: int
    tolls_amount: int
    is_weekend: bool
    weekday: str
    is_business_hours: bool
    time_of_day: str


class PredicionOutput(BaseModel):
    text: str


app = FastAPI(title="Trip Duration Prediction APP")


@app.on_event("startup")
def load_model():
    """
    Loads XGBoost regressor model used for predictions.
    """
    app.model = pkl.load(open("model/xgb_v2_for_api.pickle", "rb"))


@app.get("/")
def greet():
    """
    Greeting message visible after reaching the server.

    Returns:
        str: message.
    """
    return "Greetings from Trip Duration Prediction APP, to retrieve predictions use /predict_singe"


@app.get("/health", response_model=Health)
def health():
    """
    Get method to retrieve info if predictor is available.

    Returns:
        str: 1 if API is available.
    """
    return Health(status=1)


@app.post("/predict_single")
def model_predict(input: PredictionInput):
    """
    Takes input from get method and returns the prediction. RMSE = 3.12 minute.

    Args:
        input (PredictionInput): dictionary of features, used for prediction:
                                VendorID: int
                                passenger_count: int
                                trip_distance: float
                                RatecodeID: int
                                store_and_fwd_flag: str
                                PULocationID: int
                                DOLocationID: int
                                payment_type: int
                                tolls_amount: int
                                is_weekend: bool
                                weekday: str
                                is_business_hours: bool
                                time_of_day: str

    Returns:
        str: predicted duration of the trip.
    """
    record = {
        "VendorID": input.VendorID,
        "passenger_count": input.passenger_count,
        "trip_distance": input.trip_distance,
        "RatecodeID": input.RatecodeID,
        "store_and_fwd_flag": input.store_and_fwd_flag,
        "PULocationID": input.PULocationID,
        "DOLocationID": input.DOLocationID,
        "payment_type": input.payment_type,
        "tolls_amount": input.tolls_amount,
        "is_weekend": input.is_weekend,
        "weekday": input.weekday,
        "is_business_hours": input.is_business_hours,
        "time_of_day": input.time_of_day,
    }

    df = pd.DataFrame(record, index=[0])

    return PredicionOutput(text=str(app.model.predict(df)))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", workers=1)
