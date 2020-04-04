import pandas as pd
from datetime import datetime
from datetime import timedelta
pd.set_option('display.max_columns', None)

#read the most recent data from today (ideal to run in the evening)

#calculate today's date minus one (per reporting purposes)
today = datetime.today()
today_date = today - timedelta(days =1)
report_date = today_date.strftime('%m-%d-%Y')

daily_file_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + report_date + '.csv'
covid_data_daily_file = pd.read_csv(daily_file_url)

#select only relevant columns
covid_data = covid_data_daily_file[['Combined_Key','Confirmed','Deaths','Recovered','Active','Lat',
                       'Long_','Country_Region','Province_State']]

us_covid_data_all = covid_data[covid_data['Country_Region'] == 'US']

#handle for rows with unassigned origin locations; capture aggregates in case they want to be used in later
#analysis
is_NaN = us_covid_data_all.isnull()
row_has_NaN = is_NaN.any(axis=1)
confirmed_cases_unassigned = us_covid_data_all[row_has_NaN]['Confirmed'].sum()
deaths_unassigned = us_covid_data_all[row_has_NaN]['Deaths'].sum()
recovered_unassigned = us_covid_data_all[row_has_NaN]['Recovered'].sum()

us_covid_data = us_covid_data_all.dropna(axis=0)





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

#### Reformat data

### Aggregations

#Stacked (dual-axis graphs) [cases x deaths, time series]
#-calculate factor/ratio/rate of delay for cases -> (to) deaths
#-python integration
#Geographical map of deaths
#-filter case, deaths, cases to death ratio, per 1M deaths/cases/cases to death ratio
#-forecast of cases, deaths, and different rate metrics
#-distance traveled metric, logarithmic easing (rate of decreasing cases, prediction of “Peter out” threshold
#-5 day trailing moving averages
#-temperature, humidity, UV exposure as a feature?
#-is active cases mapped across a times series a predictor of future deaths? At what rate?









