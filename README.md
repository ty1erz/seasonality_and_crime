# Crime Seasonality Analysis

In this project, we apply timer series analysis on NYC crime dataset and Chicago crime dataset. The time series method applied are ARIMA and seasonal ARIMA. 

## Table of Contents

- [Crime Seasonality Analysis](#crime-seasonality-analysis)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Project File structure](#project-file-structure)
  - [Data Source](#data-source)
    - [On HDFS](#on-hdfs)
  - [Ingesting](#ingesting)
  - [Pre-processing NYC](#pre-processing-nyc)
    - [ETL / Cleaning](#etl--cleaning)
    - [Profiling](#profiling)
  - [Pre-processing Chicago](#pre-processing-chicago)
    - [Profiling](#profiling-1)
    - [ETL / Cleaning](#etl--cleaning-1)
  - [Analysis](#analysis)
    - [Open Jupyter Notebook on HPC](#open-jupyter-notebook-on-hpc)
    - [Timer series analysis](#timer-series-analysis)
  - [Usage](#usage)
  - [Contributing](#contributing)

## Description

Provide a more detailed explanation of the project here. This could include what problem the project solves, what technologies it uses, and any important design decisions.

## Project File structure

```
.
├── README.md
├── ana_code
│   ├── analysis_chicago_all.ipynb
│   ├── analysis_chicago_type1.ipynb
│   ├── analysis_chicago_type2.ipynb
│   ├── analysis_merged_all.ipynb
│   ├── analysis_merged_type1.ipynb
│   ├── analysis_merged_type2.ipynb
│   ├── analysis_nyc_all.ipynb
│   ├── analysis_nyc_type1.ipynb
│   └── analysis_nyc_type2.ipynb
├── data
│   ├── battery_occurrence_per_day.csv
│   ├── crime_occurrence_per_day.csv
│   ├── nypd_all.csv
│   ├── nypd_assault.csv
│   ├── nypd_larceny.csv
│   └── theft_occurrence_per_day.csv
├── data_ingest
│   ├── tz2076
│   │   └── ingest.sh
│   └── yx2021
│       └── ingest.sh
├── etl_code
│   ├── tz2076
│   │   └── cleaning.scala
│   └── yx2021
│       ├── pipeline
│       │   ├── etl1.scala
│       │   └── etl2.scala
│       └── shell
│           ├── etl1.scala
│           ├── etl2.scala
│           └── loading.scala
├── output
│   ├── chicago_all_pred.jpg
│   ├── chicago_type1_pred.jpg
│   ├── chicago_type2_pred.jpg
│   ├── merged_all_pred.jpg
│   ├── merged_type1_pred.jpg
│   ├── merged_type2_pred.jpg
│   ├── nyc_all_pred.jpg
│   ├── nyc_type1_pred.jpg
│   └── nyc_type2_pred.jpg
└── profiling_code
    ├── tz2076
    │   └── profiling.scala
    └── yx2021
        └── profiling.scala
```

## Data Source
[NYC Crime data](https://data.cityofnewyork.us/Public-Safety/NYC-crime/qb7u-rbmr)
[Chicago Crime data](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

### On HDFS
[NYC Crime Data](`hdfs://nyu-dataproc-m/user/tz2076_nyu_edu/final_project/data/nypd_raw_data.csv`)
[Chicago Crime Data](`hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/Chi_Crimes_2001_to_Present.csv`)

## Ingesting
1. Download two datasets from the source websites.
2. Upload to NYU HPC (High-Performance Computer). 
3. Put data onto HDFS for big data processing with command `hdfs dfs -put local_file_path hdfs_file_path` which can be found in folder `data_ingest`.

## Pre-processing NYC

### ETL / Cleaning
The input data is located at: `hdfs://nyu-dataproc-m/user/tz2076_nyu_edu/final_project/data/nypd_raw_data.csv`.
The code reads in the data that I provide and does data cleaning job through spark.

Below are the column that I selected to be used in further work:
```
|-- id: integer (nullable = true)
|-- date: date (nullable = true)
|-- typeId: integer (nullable = true)
|-- typeDesc: string (nullable = true)
```

This dataframe is saved to be used in the profiling process, located at `hdfs://nyu-dataproc-m/user/tz2076_nyu_edu/final_project/data/nypd_cleaned.csv`.

After that, I aggregate information from the data.
1. In extraction stage, I filter out two extra dataframes which only contains either LARCENY crimes or ASSAULT crimes with regular expression based methods.
2. Use the original dataframe which contains all crime types and these two dataframes, I aggregate the number of crimes in each day with `groupby("date")` separately.
3. Save those data separately in the directory `final_project/data`.

Here are the files of processed data:
```
|-- `nyc_all.csv`
|-- `nyc_assault.csv`
|-- `nyc_larceny.csv`
```

These three aggregated dataframes contains these two columns:
```
|-- date: date (nullable = true)
|-- count: integer (nullable = true)
```

All work could be done by running `spark-shell --deploy-mode client -i  cleaning.scala` on HPC.

### Profiling
This stage explores the intermediate data `nypd_cleaned.csv` from cleaning stage and generate some information about the dataset.

1. Cast `id` and `typeId` into INTEGER.
2. Cast `date` into type date object with the format `MM/dd/yyyy`.
3. Find which week the date was in, and store the information as `week_number`.
4. Get distinct value in `date` and `typeId`.
5. Check the mean value for `week_number`.
6. Group the records by `typeId` and `typeDesc` and aggregate the counts of them. 

All work could be done by running `spark-shell --deploy-mode client -i  profiling.scala` on HPC.

## Pre-processing Chicago

### Profiling
Use file in `profiling/yx2021/profiling.scala` to profile the dataset. Steps include:
1. Show dataset schema. 
2. Count distinct values. 
3. Find all distinct values for categorical variables.
### ETL / Cleaning
Use files in `etl_code/yx2021/` to do data cleaning and transforming. The files in folder `pipeline` and folder `shell` perform same functionality. Each file in folder `pipeline` can be executed as a whole scala file with command `spark-shell --deploy-mode client -i  FILENAME.scala`. On the other hand, files in folder `shell` were written for user to run line by line in an scala interactive shell so that user could get a sense about how the data set is processed. Cleaning and transforming steps include:

`etl1.scala`
1. Drop unnecessary columns.
2. Convert date filed into date object.
3. Drop row that contains NaN values. 
4. Write the cleaned dataset to `crime_type_data.csv`

`etl2.scala`
1. Aggregate data on date to get the crime occurrence on each day. 
2. Filter data on crime type "theft" and then aggregate data on date to get `theft_occurrence_per_day.csv`. 
3. Filter data on crime type "battery" and then aggregate data on date to get `battery_occurrence_per_day.csv`. 

*`loading.scala`

Code in this file is ued to open scala shell and load raw data `Chi_Crimes_2001_to_Present.csv` into a dataframe in scala.

## Analysis

### Open Jupyter Notebook on HPC
[HPC Jupyter Tutorial](https://sites.google.com/nyu.edu/nyu-hpc/hpc-systems/cloud-computing/dataproc#h.z93pn6133hwc)
1. Install google cloud `gcloud` on your local machine. [gcloud installation tutorial](https://cloud.google.com/sdk/docs/install)
2. In your terminal, run `gcloud auth login`. A web page will be prompt to ask you to login your authorized google account. 
3. On HPC, your home directory, run command `jupyter-notebook`. It will prompt a URL for you to copy and paste in a new page. 
4. From the output produced by jupyter-notebook, obtain the port number that the notebook is running on. Run below command in your terminal and replaced both `PORT` of this command with the port number: 
   
`gcloud compute ssh nyu-dataproc-m --project hpc-dataproc-19b8 --zone us-central1-f -- -N -L PORT:localhost:PORT`

For example, if notebook is running on 8888, the command should be:

`gcloud compute ssh nyu-dataproc-m --project hpc-dataproc-19b8 --zone us-central1-f -- -N -L 8888:localhost:8888`

5. Copy and paste the URL you obtain from step 3 into a new web page, now you should see the interface of jupyter notebook.

### Timer series analysis

In total we have 3(cities) * 3(crime types) = 9 notebooks. Cites include Chicago, NYC, merged of both. Crime types include all crime types as whole, type 1 (theft/larceny), and type 2 (battery/assault). We sued type 1 and type 2 because NYC and Chicago encode the crime types differently. Based on our research on crime type encoding system, we recognize theft and larceny as type 1 and battery and assault as type 2. 

All notebooks contain same analysis pipeline include below steps:
1. Import required package and resolve dependency.
2. Load datasets, initialized dataset specific variables, setup data frame (convert to date object and set as index).
3. Data visualization of crime frequency in different scales (daily, weekly, monthly). 
All following steps is performed on monthly data, because the monthly data show more clear pattern. 
4. Check whether dataset is stationary with `adfuller_test()`, augmented Dickey-Fuller test.
5. Performing differencing in shift of 12 month (Compute the difference between each observation and the corresponding observation from the same month in the previous year) to remove the macro trend component from the data.
6. Perform `adfuller_test()` again on the data after differencing and check whether it is stationary. 
7. Create `ACF` and `PACF` plot to find potential lag value with high auto-correlation and partial auto-correlation.
8. Split dataset into training and testing set. 
9. Training the seasonal ARIMA model and optimize it with parameter tunning. 
10. Show model summary.
11. Use the model to predict on the test and visualize the result of true values and predicted values. 
12. Output the plots in folder `output`.
13. Evaluate each model on test set by RMSE (root mean square error). 


## Usage

The model we trained in this project could be used to predict future crime peaks. Specifically, our model works best on predicting type1 crime. On the other hand, model that predict single city performs better than predicted on merged dataset. 

## Contributing

If interested, follow above instruction to reproduce the result and feel free to do modification and create a issue or pull request if you got any interesting insights!

