# NYC_TripDuration_model

**Problem**: predict trip duration  
**Approach**: build an MVP/POC to predict trip duration using one month of data, scale on scraping and adding nont existing datasets.

  
**path**:  
- *write a simple scraper to collect one data from/till period. for mvp(PoC) use only 1 month data.  (completed)*
- perform an EDA and enrich data using given data dictionary
- create a pipeline to prep the data, firstly save data localy (note: dont forget to gitignore data folders). improvement save files in google cloud, use data versioning (DVC) and tags
- create pipelines to check 2-3 models on finalized data.
- save the best model, create an API to input features and return the prediction (firstly as a single input, later expand to multiple input-output)
- **optional**: create a local database, to write the requests and save the model outputs.

**Definition of done**:  
- github repository fixed: Readme file finalized.
- Working data prep and modeling tool to download and create model
- Working API to receive and output expected trip duration
- optional: local database with history of all inquiries to the API and results.  

** Future improvements ** 
- implement exception handling
- implement debugger
