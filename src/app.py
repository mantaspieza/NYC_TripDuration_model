import pandas as pd
import numpy as np
import pickle as pkl
import uvicorn

# import asyncio

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


class InferenceOutput(BaseModel):
    text: str


app = FastAPI(title="Trip Duration Prediction APP")


@app.on_event("startup")
def load_model():
    app.model = pkl.load(
        open("NYC_TripDuration_model/model/xgb_v1_for_api.pickle", "rb")
    )


@app.get("/")
def greet():
    return "Greetings from Trip Duration Prediction APP"


@app.get("/health", response_model=Health)
def health():
    return Health(status=1)


@app.post("/predict_single")
def model_predict(input: PredictionInput):
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

    return InferenceOutput(text=str(app.model.predict(df)))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", workers=1)
