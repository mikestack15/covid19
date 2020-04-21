import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
import world_bank_data as wb
import seaborn as sns
pd.set_option('display.max_columns', None)

#### Read in data
#### file processor function can eventually be used to query data from previous daily files
####read the most recent data from today (minus 1 day to allow reports to catch up from previous day)
today = datetime.today()
today_date = today - timedelta(days = 1)
report_date = today_date.strftime('%m-%d-%Y')

#read in daily file (Johns Hopkins data set)
daily_file_data = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + report_date + '.csv')

#read in times series data (Johns Hopkins data set)
time_series_deaths = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
time_series_cases = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')

####Clean up data: (daily file data reformatting)
#most countries are not reporting to the specific provinence/state/city of case/death origin
#thus, let's define a function that aggregates 'Country_Region' counts > 1
country_region_row_count = daily_file_data['Country_Region'].value_counts()
#country_region_row_count.head(12)


def country_aggregator(daily_file):
    granular_country_data = pd.DataFrame(columns=daily_file_data.columns)
    generalized_country_data = pd.DataFrame(columns=['Country_Region', 'Confirmed', 'Deaths', 'Recovered', 'Active'])
    for country in daily_file['Country_Region'].unique():
        #filter down to a specific Country_Region
        dat = daily_file[daily_file['Country_Region'] == country]
        if len(dat) > 1:
            #add to granular country dataframe (can be used for future analysis)
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

#only grab the cleaned up aggregated file
country_aggregated_data = country_aggregator(daily_file_data)[0]


#note data being added late (China on 4/16/2020 reporting > 1200 deaths)

###Scraping data from API's and joining data from the web
#scrape datasets from the web (population, hospital beds per capita, climate),
# and join them into our current covid-19 dataset as features

#join in population data sets (world bank data)
pop_data = wb.get_series('sp.pop.totl',mrv=1)
pop_data = pop_data.to_frame().reset_index()

#join in hospital beds per capita data (world bank data)
hosp_bed_data = wb.get_series('sh.med.beds.zs',mrv=1)
hosp_bed_data = hosp_bed_data.to_frame().reset_index()

#twitter API?
#tweepy




#clean up data:
# delete unnecessary columns
# remove NA's
# rename columns
# join columns to corresponding countries [via fuzzy match])









#check for nan's in dataframe columns
nan_values = country_aggregated_data.isna()
nan_columns = nan_values.any()
columns_with_nan = country_aggregated_data.columns[nan_columns].tolist()
print(columns_with_nan)


#add calculated columns
country_aggregated_data['Fatality_Rate'] = country_aggregated_data['Deaths'] / country_aggregated_data['Confirmed']



#####****End product goals/analysis/models/walkthroughs
#Stacked (dual-axis graphs) [cases x deaths, time series]
#-calculate factor/ratio/rate of delay for cases -> (to) deaths
#Geographical map of deaths
#***write to GCP cloud storage
#-filter case, deaths, cases to death ratio, per 1M deaths/cases/cases to death ratio
#-forecast of cases, deaths, and different rate metrics
#-distance traveled metric, logarithmic easing (rate of decreasing cases, prediction of “Peter out” threshold
#-5 day trailing moving averages
#-temperature, humidity, UV exposure as a feature?
#-is active cases mapped across a times series a predictor of future deaths? At what rate?
#looping aggregator function on previous daily files to show expansion of cases/deaths
#sentiment analysis on public opinion?








