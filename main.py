#Author: Mike Stack
#Last Updated: 5/26/2020
#this script ingests all of the data sources (*twitter data still needs to be added to process), performs necessary
#transformations, and produces a 3rd degree polynomial forecast model for predicting the next 14 days of cases.
#The data is then outputed as objects onto GCP storage (BigQuery) and read into corresponding visualization suites

import pandas as pd
import os as os
from datetime import datetime, timedelta
import world_bank_data as wb
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
#pd.set_option('display.max_columns', None)

os.chdir('C:/Users/16308/Desktop/skeleton_nf_repo/covid19')


#read in data sets
country_agg_data_filePath = 'C:/Users/16308/Desktop/skeleton_nf_repo/covid19/country_agg_data.csv'
case_surge_time_series_data_filePath = 'C:/Users/16308/Desktop/skeleton_nf_repo/covid19/case_surge_time_series_data.csv'
forecasted_cases_filePath = 'C:/Users/16308/Desktop/skeleton_nf_repo/covid19/forecasted_cases.csv'

#create list of file path locations
filePathNames = [country_agg_data_filePath,case_surge_time_series_data_filePath,forecasted_cases_filePath]

for file in filePathNames:
    if os.path.exists(file):
        os.remove(file)
    else:
        print('file does not exist')

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
def daily_file_aggregator(daily_file, country_region=None):
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
country_aggregated_data = daily_file_aggregator(daily_file_data)[0]
granular_data_united_states = daily_file_aggregator(daily_file_data, country_region='US')[1]
#note, some data can sometimes be added/reported late (i.e. China on 4/16/2020 reporting > 1200 deaths)


###Getting data from API's (world bank API [world_bank_data]

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
hosp_bed_data = hosp_bed_data.rename(columns={'sh.med.beds.zs':'hosp_beds_per_1000_people',
                                              'Year':'MostRecentYearHospBedInfoCollected',
                                              'Country':'Country_Region'}).drop(['Series'],axis=1)

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

#merge first reported case
country_aggregated_data = pd.merge(country_aggregated_data,first_case_data,how='left',on='Country_Region')

#match country codes from pop data/hospital bed per 1000
#tables with keys aligned properly
Country_Region_Key_WB_Pop = pd.read_csv('Country_Region_Key_Table_WB_Pop.csv')
Country_Region_Key_WB_Hosp_Bed = pd.read_csv('Country_Region_Key_Table_WB_Hosp_Bed.csv')
Continent_Mapping_Key = pd.read_csv('Continent_Mapping.csv')

#join to pop data and hosp_bed country_region key dataframes
country_aggregated_data = pd.merge(country_aggregated_data,Country_Region_Key_WB_Pop, how='left', on='Country_Region')
country_aggregated_data = pd.merge(country_aggregated_data,Country_Region_Key_WB_Hosp_Bed, how='left', on='Country_Region')

#rename columns
pop_data = pop_data.rename(columns={"Country_Region":"wb_pop_Country_Region"})
hosp_bed_data = hosp_bed_data.rename(columns={"Country_Region":"wb_hosp_bed_Country_Region"})
#merge pop/hosp bed data
country_aggregated_data = pd.merge(country_aggregated_data,pop_data,how='left',on='wb_pop_Country_Region')
country_aggregated_data = pd.merge(country_aggregated_data,hosp_bed_data,how='left',on='wb_hosp_bed_Country_Region')
#merge continent (used for as zoom-in dashboard filter)
country_aggregated_data = pd.merge(country_aggregated_data,Continent_Mapping_Key,how='left', on='Country_Region')
#drop countries with no data/match found
country_aggregated_data = country_aggregated_data.dropna()
country_aggregated_data = country_aggregated_data.drop(['wb_pop_Country_Region','wb_hosp_bed_Country_Region'],axis=1)

#add calculated columns
#fatality rate
country_aggregated_data['Fatality_Rate'] = country_aggregated_data['Deaths'] / country_aggregated_data['Confirmed']
#per one million ratio [population / 1000000]
per_Mil_ratio = country_aggregated_data['Population'] / 1000000
#cases per 1 million people
country_aggregated_data['cases_per_mil_people'] = country_aggregated_data['Confirmed'] / per_Mil_ratio
#deaths per 1 million citizens
country_aggregated_data['deaths_per_mil_people'] = country_aggregated_data['Deaths'] / per_Mil_ratio
#recovery rate
country_aggregated_data['Recovery_Rate'] = country_aggregated_data['Recovered'] / country_aggregated_data['Confirmed']

#import total tests by country (worldometer.com)
#tests per 1 million citizens

#time series dataframe transformations
time_series_cases = time_series_cases.rename(columns = {'Country/Region':'Country_Region'})
pop_data_key = country_aggregated_data[['Country_Region','Population']]
time_series_cases = pd.merge(time_series_cases,country_aggregated_data[['Country_Region','Population']],
                             on='Country_Region',how='left')
#list of countries to aggregate time series info
countries = time_series_cases['Country_Region'].unique()
def cases_time_series_aggregator(days = 50):
    case_surge_data = pd.DataFrame(columns=['Country_Region', 'Date', 'Cases', 'Cases_per_Million',
                                            'case_delta_days','case_percentage_x_days_increase'])
    for country in countries:
        country_data = time_series_cases[time_series_cases['Country_Region'] == country]
        country_population = country_data.iloc[:,-1].iloc[0]
        country_data = country_data.drop(['Population'],axis=1)
        if len(country_data) > 1:
            Cases = country_data[country_data.columns[-days:]].sum()
            Dates = country_data[country_data.columns[-days:]].columns
            country_ts_data = pd.DataFrame({'Date':Dates,'Cases':Cases})
        else:
            country_ts_data = country_data[country_data.columns[-days:]].transpose().reset_index()
            country_ts_data.columns = ['Date','Cases']
        country_ts_data['Cases_per_Million'] = country_ts_data['Cases'] / (country_population / 1000000)
        case_delta_days = country_ts_data['Cases'].iloc[-1] - country_ts_data['Cases'].iloc[0]
        if country_ts_data['Cases'].iloc[0] < 1:
            case_percentage_x_days_increase = country_ts_data['Cases'].iloc[-1]
        else:
            case_percentage_x_days_increase = (country_ts_data['Cases'].iloc[-1] - country_ts_data['Cases'].iloc[0]) / \
                                       country_ts_data['Cases'].iloc[0]
        country_case_surge_data = pd.DataFrame({'Country_Region': country,
                                    'Date': country_ts_data['Date'],
                                    'Cases': country_ts_data['Cases'],
                                    'Cases_per_Million': country_ts_data['Cases_per_Million'],
                                    'case_delta_days': case_delta_days,
                                    'case_percentage_x_days_increase': case_percentage_x_days_increase})
        case_surge_data = case_surge_data.append(country_case_surge_data)
    return case_surge_data



#last 14 day surge data
case_surge_time_series_data = cases_time_series_aggregator(days=14)

#forecast by country

case_surge_time_series_data_rf = cases_time_series_aggregator(days=50)
case_surge_time_series_data_rf = case_surge_time_series_data_rf[['Country_Region','Date','Cases']]
#separate actual collected values from eventually imputed forecasted values
case_surge_time_series_data_rf['Value_Type'] = 'Actual Value'
countries = case_surge_time_series_data_rf['Country_Region'].unique()
#
#country = 'US'
def forecast_by_country(forecast_days=14):
    full_ts_data = pd.DataFrame(columns=['Country_Region', 'Date', 'Cases', 'Value_Type'])
    for country in countries:
        country_ts_data = case_surge_time_series_data_rf[case_surge_time_series_data_rf['Country_Region']==country]
        #numeric value for date (range of last x days), array formats for both dates and cases
        dates = np.array(range(0, len(country_ts_data['Date'])))
        forecast_dates = np.array(range(len(dates),len(dates) + forecast_days))
        cases = np.array(country_ts_data['Cases'])
        #reformat axis and array format for model fitting
        dates = dates[:, np.newaxis]
        cases = cases[:, np.newaxis]
        forecast_dates = forecast_dates[:, np.newaxis]
        dates = dates.reshape(-1,1)
        forecast_dates = forecast_dates.reshape(-1,1)
        # Fitting Polynomial Regression to the dataset
        poly_reg = PolynomialFeatures(degree=3)
        X_poly = poly_reg.fit_transform(dates)
        pol_reg = LinearRegression()
        pol_reg.fit(X_poly, cases)
        #forecast values with fitted polynomial model
        country_forecast_values = pol_reg.predict(poly_reg.fit_transform(forecast_dates)).tolist()
        country_forecast_values = [item[0] for item in country_forecast_values]
        last_collected_date_value = datetime.strptime(country_ts_data['Date'].tail(1).iloc[0] , '%m/%d/%y') \
                                    + timedelta(days=1)
        actual_forecast_dates = pd.date_range(start=last_collected_date_value,periods=len(forecast_dates))
        #create forecast output dataframe
        forecast_output = pd.DataFrame({'Date':actual_forecast_dates,
                                        'Cases':country_forecast_values},
                                       columns=['Date','Cases'])
        forecast_output['Country_Region'] = country
        forecast_output['Value_Type'] = 'Forecast Value'
        forecast_output = forecast_output[['Country_Region','Date','Cases','Value_Type']]
        #combine actual data with forecasted values for visualization down the pipeline
        full_country_ts_data = pd.concat([country_ts_data,forecast_output],ignore_index=True)
        full_ts_data = full_ts_data.append(full_country_ts_data)
    return full_ts_data

forecasted_cases = forecast_by_country(forecast_days=14)

forecasted_cases.to_csv('forecasted_cases.csv')
country_aggregated_data.to_csv(('country_agg_data.csv'))
case_surge_time_series_data.to_csv('case_surge_time_series_data.csv')


#neural network model

#record model results/error, and retrain models

#build tables for tableau to illustrate model accuracy, retrained model improvement




