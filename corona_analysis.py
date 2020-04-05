import pandas as pd
from datetime import datetime
from datetime import timedelta
pd.set_option('display.max_columns', None)

#read the most recent data from today (ideally, evening time for most-oup to date
# (minus 1 day to allow reports to catch up from previous day)

#calculate today's date minus one (per reporting purposes)
today = datetime.today()
today_date = today - timedelta(days =1)
report_date = today_date.strftime('%m-%d-%Y')

#daily file
daily_file_data = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + report_date + '.csv')

#times series data
time_series_deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
time_series_cases = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')

#daily file data reformatting
#most countries are not reporting to the specific provinence/state/city of case/death origin
#thus, let's define a function that aggregates 'Country_Region' counts > 1
country_region_row_count = daily_file_data['Country_Region'].value_counts()
#country_region_row_count.head(12)
#country = 'US'
#country = 'Kuwait'

def country_aggregator(daily_file):
    granular_country_data = pd.DataFrame(columns=daily_file_data.columns)
    generalized_country_data = pd.DataFrame(columns=['Country_Region', 'Confirmed', 'Deaths', 'Recovered', 'Active'])
        for country in daily_file['Country_Region'].unique():
            #filter down to a specific country_region
            dat = daily_file[daily_file['Country_Region'] == country]
            if len(dat) > 1:
                #add to granular country dataframe (will be used for later analysis
                granular_country_data = granular_country_data.append(dat)
                #aggregate confirmed cases, deaths, recovered, active
                country_data = pd.DataFrame({'Country_Region': [country],
                                'Confirmed': [dat['Confirmed'].sum()],
                                'Deaths': [dat['Deaths'].sum()],
                                'Recovered': [dat['Recovered'].sum()],
                                'Active': [dat['Active'].sum()]})
                generalized_country_data = generalized_country_data.append(country_data)
            else:
                generalized_country_data = generalized_country_data.append(dat[['Country_Region','Confirmed',
                                                                                'Deaths','Recovered','Active']])
        return generalized_country_data, granular_country_data


country_aggregated_data = country_aggregator(daily_file_data)[1]













covid_data_daily_file = pd.read_csv(daily_file_url)

#select only relevant columns
covid_data_all = covid_data_daily_file[['Combined_Key','Confirmed','Deaths','Recovered','Active','Lat',
                       'Long_','Country_Region','Province_State']]

### Aggregations

country_data = covid_data_all.groupby('Country_Region')[['Country_Region','Deaths','Confirmed']]






#join in population data sets

#join in historical weather data sets



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









