import pandas as pd
import datadotworld as dw

import requests
#read flat file form 3/19/2020
corona_data = pd.read_csv('COVID-19 Cases.csv')


url = 'https://api.data.world/v0/datasets/covid-19-data-resource-hub/covid-19-case-counts'
intro_dataset = dw.load_dataset(url)

#owner is "covid-19-data-resource-hub", id is "covid-19-case-counts"


covid-19-data-resource-hub/covid-19-case-counts










