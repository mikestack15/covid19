import pandas as pd
from datetime import datetime
from datetime import timedelta

#read the most recent data from today (ideal to run in the evening)

#calculate today's date minus one (per reporting purposes)
today = datetime.today()
today_date = today - timedelta(days =1)
report_date = today_date.strftime('%m-%d-%Y')

daily_file_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + report_date + '.csv'
covid_data_daily_file = pd.read_csv(daily_file_url)


#confirmed cases
confirmed_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv'
covid_data_confirmed_cases = pd.read_csv(confirmed_url)
#covid deaths
deaths_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv'
covid_deaths = pd.read_csv(deaths_url)
#recovered covid cases/patients
recovered_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv'
covid_recovered = pd.read_csv(recovered_url)
#global confirmed cases
confirmed_cases_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
covid_global_cases = pd.read_csv(confirmed_cases_global_url)
#global deaths
deaths_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
covid_global_deaths = pd.read_csv(deaths_global_url)

#python virtual environment
#### in terminal
#### pipenv --venv











