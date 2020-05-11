import pandas as pd
from datetime import datetime, timedelta
import world_bank_data as wb
import fuzzywuzzy as fw
from sklearn.ensemble import RandomForestRegressor
from fuzzywuzzy import process, fuzz

#pd.set_option('display.max_columns', None)

#### Read in COVID-19 data via raw github extracts
####read the most recent data from today (minus 1 day to allow reports to catch up from previous day)
today = datetime.today()
today_date = today - timedelta(days = 1)
report_date = today_date.strftime('%m-%d-%Y')
#master branch
master_repo = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/'
#read in daily file (Johns Hopkins data set)
daily_file_data = pd.read_csv(master_repo + 'csse_covid_19_daily_reports/' + report_date + '.csv')
#read in times series data (Johns Hopkins data set)
time_series_deaths = pd.read_csv(master_repo + 'csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
time_series_cases = pd.read_csv(master_repo + 'csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')


#granular_data_united_states[granular_data_united_states['Country_Region'] == 'US']
####Clean up data: (daily file data reformatting)
#most countries are not reporting to the specific provinence/state/city of case/death origin
country_region_row_count = daily_file_data['Country_Region'].value_counts()
#country_region_row_count.head(12)
#thus, let's define a function that aggregates countries with regions > 1 they are reporting data for
def file_aggregator(daily_file, country_region=None):
    granular_country_data = pd.DataFrame(columns=daily_file_data.columns)
    generalized_country_data = pd.DataFrame(columns=['Country_Region', 'Confirmed',
                                                     'Deaths', 'Recovered', 'Active'])
    for country in daily_file['Country_Region'].unique():
        #filter down to a specific Country_Region
        country_data = daily_file[daily_file['Country_Region'] == country]
        if len(country_data) > 1:
            #add to granular country dataframe (can be used for future analysis)
            granular_country_data = granular_country_data.append(country_data)
            #aggregate confirmed cases, deaths, recovered, active
            country_data = pd.DataFrame({'Country_Region': [country],
                                'Confirmed': [country_data['Confirmed'].sum()],
                                'Deaths': [country_data['Deaths'].sum()],
                                'Recovered': [country_data['Recovered'].sum()],
                                'Active': [country_data['Active'].sum()]})
            generalized_country_data = generalized_country_data.append(country_data)
        else:
            generalized_country_data = generalized_country_data.append(country_data[['Country_Region','Confirmed',
                                                                            'Deaths','Recovered','Active']])
    granular_country_data = granular_country_data[granular_country_data['Country_Region'] == country_region]
    return generalized_country_data, granular_country_data

#only grab the cleaned up aggregated file
country_aggregated_data = file_aggregator(daily_file_data)[0]
granular_data_united_states = file_aggregator(daily_file_data,country_region='US')[1]
#note, some data can sometimes be added/reported late (i.e. China on 4/16/2020 reporting > 1200 deaths)


###Getting data from API's (world bank API [world_bank_data] & twitter API [tweepy] and joining to COVID data
#grab datasets from the web (population, hospital beds per 1000 people)
#and join them into our current covid-19 dataset as features
#fetch population by country data sets (world bank data)
pop_data = wb.get_series('sp.pop.totl',mrv=1).to_frame().reset_index()
#rename columns so they can be joined/fuzzy matched to COVID data, delete unnecessary columns
pop_data = pop_data.rename(columns={'sp.pop.totl':'Population',
                                    'Country':'Country_Region'}).drop(['Series','Year'], axis=1)
#fetch hospital beds per capita data (world bank data), remove countries with nan/missing data
hosp_bed_data = wb.get_series('sh.med.beds.zs').to_frame().reset_index().dropna()
#keep only the most recent year when this metric was captured (per country)
hosp_bed_data = hosp_bed_data.drop_duplicates('Country',keep='last')
#rename columns, drop unnecessary columns
hosp_bed_data = hosp_bed_data.rename(columns={'sh.med.beds.zs':'beds_per_1000_people',
                                              'Year':'MostRecentYearCollected',
                                              'Country':'Country_Region'}).drop(['Series'],axis=1)
#fuzzy match the countries?

def match_term(term, list_names, min_score=0):
    max_score = -1
    max_name = ""
    for term2 in list_names:
        score = fuzz.partial_ratio(term,term2)
        if (score > min_score) & (score > max_score):
            max_name = term2
            max_score = score
    return (max_name, max_score)

countries_jh = list(country_aggregated_data['Country_Region'])
countries_wb = list(pop_data['Country_Region'])

for i in countries_jh:
    print(i, match_term(i, countries_wb, 50))

#match country codes from pop data/hospital bed per 1000
country_covid_pop = pd.merge(country_aggregated_data,pop_data,how='left',on='Country_Region')
country_covid_hosp_bed = pd.merge(country_aggregated_data,hosp_bed_data,how='left',on='Country_Region')

countries_wo_match = list(country_covid_pop[country_covid_pop.isna().any(axis=1)]['Country_Region'])


#fuzzy_match






#add calculated columns

#fatality rate
country_aggregated_data['Fatality_Rate'] = country_aggregated_data['Deaths'] / country_aggregated_data['Confirmed']

#cases per 1 million citizens

#deaths per 1 million citizens

#import total tests by country (worldometer.com)


#extract first reported case date by country from timeseries file
countries = time_series_cases['Country/Region'].unique()
column_name_list = time_series_cases.iloc[:,4::].columns #only want after 5th column (column where dates begin)
first_case_data = pd.DataFrame(columns=['Country_Region','First_Confirmed_Case'])
for country in countries:
    row = time_series_cases[time_series_cases['Country/Region'] == country]
    row_case_check = row.iloc[:,4::]
    bool_list = (row_case_check > 0).any()
    res = list(filter(lambda i: bool_list[i], range(len(bool_list))))[0]
    first_case = column_name_list[res]
    first_case_country = pd.DataFrame({'Country_Region': [country],
                                       'First_Confirmed_Case': [first_case]})
    first_case_data = first_case_data.append(first_case_country)


#####****End product goals/analysis/models/walkthroughs
#Stacked (dual-axis graphs) [cases x deaths, time series]
#-filter case, deaths, cases to death ratio, per 1M deaths/cases/cases to death ratio
#-forecast of cases, deaths, and different rate metrics
#logarithmic easing (rate of decreasing cases, prediction of “Peter out”/apex threshold
#-5 day trailing moving averages
#-temperature, humidity, UV exposure as a feature?
#-are active cases mapped across a times series a predictor of future deaths? At what rate?
#sentiment analysis on public opinion via twitter








