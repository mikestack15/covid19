import pandas as pd
from datetime import datetime

today_date = datetime.today().strftime('%m-%d-%Y')
url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/' + today_date + '.csv'
covid19_data = pd.read_csv(url)


#owner is "covid-19-data-resource-hub", id is "covid-19-case-counts"












