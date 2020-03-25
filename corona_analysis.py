import pandas as pd
from datetime import datetime

#read the most recent data from today (ideal to run in the evening)

today_date = datetime.today().strftime('%m-%d-%Y')
daily_file_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + today_date + '.csv'
covid_data_daily_file = pd.read_csv(daily_file_url)


#owner is "covid-19-data-resource-hub", id is "covid-19-case-counts"

#confirmed cases
confirmed_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv'
covid_data_confirmed_cases = pd.read_csv(confirmed_url)
#covid deaths
deaths_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv'
covid_deaths = pd.read_csv(deaths_url)
#recovered covid cases/patients
recovered_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv'

confirmed_cases_global_url = 'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'

deaths_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'














