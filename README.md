# Song Rank Prediction Based on Debut Week Rankings

The installation and deployment guide for replicating the results obtained in the 'Song Rank Prediction Based on Debut Week Rankings'
Applied Machine Learning coursework report. 

The README file is structured as follows:
1. [Installation](#installation)
2. [Data Preparation](#data-preparation)
3. [Exploratory Data Analysis](#exploratory-data-analysis)
4. [Hyperparameter Search](#hyperparameter-search)
5. [Model Evaluation](#model-evaluation)
> ❗ Step 2 [Data Preparation](#data-preparation) can be skipped as all the files to perform the following steps have already been
included in the directory. <br> Additionally, we suggest skipping this step since models may have different outcome results than those in the report. 
> This is due to variation in the training and testing datasets as a result of manual adjustments, as well as variation in metrics 
> collected from the Spotify API.
> 
## Installation

To run the project, ensure that all the required Python dependencies are installed by running
```
pip install -r requirements.txt
```
The project is intended to work with Python version 3.11.5, which can be installed from the official [Python release page](https://www.python.org/downloads/) 

## Data Preparation

### Follower information extraction

In order to obtain the followers of songs' artists, carry out the following steps:

1. Follow the steps in [Spotify Web API documentation](https://developer.spotify.com/documentation/web-api/tutorials/getting-started)
to claim an access token.
2. Locate to `Data_Preparation` directory and in terminal run:
```
python spotify_api.py [access token] [path to Spotify_Dataset_V3.csv]
```
The script will produce a `spotify_dataset_updated.csv` file in the `Data` directory. The produced dataset is identical to the original one
with an additional column for followers respective to the artist in 'Artists (Ind.)' column.

### Missing Rank Replacement

To retrieve some of the missing ranks in the dataset, carry out the following steps:

1. Ensure that the `spotify_dataset_updated.csv` file is in the `Data` directory.
2. Locate to `Data_Preparation` directory and in terminal run:
```
python missing_dates.py
```
The script will produce a `spotify_dataset_updated_2.csv` file in the `Data` directory.

### Dataset Restructuring

To create a dataset, where each dataset point represents a unique song with its
respective information, carry out the following steps:

1. Start the Jupyter notebook server by running in terminal:
```
jupyter notebook
```
2. Locate to `Data_Preparation` directory, open the `Spotify_14_Day_Dataset.ipynb`
notebook and run its cells. 

The notebook will produce :
- The restructured dataset  - `songs_dataset.csv`
- Training set - `training.csv`
- Testing set - `testing.csv`
- Normalised training set -`training_normalised.csv`
- Normalised testing set - `testing_normalised.csv`

all located in `Data` directory.


## Exploratory Data Analysis

In order to produce the plots from Section 3 - Exploratory Data Analysis, 
carry out the following steps:

1. Start the Jupyter notebook server by running in terminal:
```
jupyter notebook
```
2. Open the `EDA.ipynb` notebook and run its cells. The plots will be displayed in the notebook and saved in the `report/figures/` directory.


## Hyperparameter Search

In order to produce the results of the grid search for all models as described
in Section 4.3, carry out the following steps:
1. Start the Jupyter notebook server by running in terminal:
```
jupyter notebook
```
2. Open the `Hyperparameter_Search_CV.ipynb` notebook and run its cells. The metrics for each set of hyperparameters  will be saved in a 
`Data/Hyperparameter_Search/`directory in a separate file for each model.

3. Open the `Hyperparameter_Search_Visualisation.ipynb` notebook and run its cells
to produce the plots for model training as illustrated in Appendix B. The plots will be displayed in the notebook and saved in the `report/figures/` directory.


## Model Evaluation

In order to produce the model evaluation results as described in Section 5, carry out the 
following steps:

1. Start the Jupyter notebook server by running in terminal:
```
jupyter notebook
```
2. Open the `Model_Evaluation.ipynb` notebook and run its cells, which will report the results for Baselines 1,2 and Models 1-4.
