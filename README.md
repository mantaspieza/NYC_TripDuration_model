# New York City Yellow Taxi Trip Duration Prediction

### Hypothetical Business problem
The current trip duration prediction model does not perform well during the Christmas period (24th 25th and 26th of December.
  
### Hypothetical Business task
To create a specialized regression model (MVP) to predict trip duration during Christmas (24th to 26th of December) using data from the past 5 years (2017-2021).  

Definition of MVP model:
- Should perform better than Linear regression using the same data.
- Should be easy to adjust and optimize, meaning training and optimization runtime should be reasonable vs results.

### Definition of done
* Automated data extraction from the Yellow Taxi webpage.
* Automated data transformation is performed.
* Best performing algorithm identified, trained, and saved.
* APP created using the best-performing algorithm to return trip-duration prediction from a single inquiry.

### High-level overview and results
XGBoost Regressor model was chosen as best suited candidate to perform the task. The results were quite surprising, as the model can predict the trip duration using the test dataset evaluating on MAE metric with approximately a 2-minute 14-second error. RMSE on the test dataset returns approximately a 3-minute 21-second error. 

## Structure of the repository
#### Model:
The folder holds the pre-trained models used by FastAPI.
#### src:
Folder used to hold python scripts:
* app.py: Creates and launches FastAPI application on the local machine.
* scraper.py: extracts required datasets from the [New York city government webpage](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and saves them locally. 
* transformer.py: reads in locally saved extracted datasets and transforms them using the insights and assumptions defined in **engineering** notebook; saves them locally
* prep_data: orchestrates extraction and transformation (data preparation) for model creation.

## Roadmap
completed:  
[x] Scraper created and documented to extract data from New York City government webpage.  
[x] Exploratory data analysis performed to identify outliers, perform manual feature selection, and feature engineering. (engineering.ipynb)  
[x] Transformer created and documented to apply transformations on raw data.  
[x] Year-to-year comparison of datasets performed to identify if different year datasets can be used to create a Trip duration prediction model. (modeling.ipynb)
[x] Algorithm comparison performed, identifying the best suitable candidates.  (modeling.ipynb)  
[x] Recursive Feature Selection test implemented, and model optimization Hyperparameter optimization completed. (modeling.ipynb)   
[x] FastAPI App created and documented to predict the duration of the trip.



## License

[MIT](https://choosealicense.com/licenses/mit/)

## Project Status

Project is completed. Minor updates are expected.