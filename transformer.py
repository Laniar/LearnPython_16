import pandas as pd

def get_data_csv(csv_folder,important_columns):  
    csv_data = pd.read_csv(csv_folder,usecols=important_columns)
    return csv_data

def transform_data(csv_data,dims,measures):
    csv_data['Last Update'] = pd.to_datetime(csv_data['Last Update']).dt.date
    aggr_data = csv_data.groupby(dims).agg(measures)
    return aggr_data

def get_currency_by_date():
    currency_list = ['USD','EUR','CHF','CNY','SEK','UAH','JPY']
    currency_columns = ['Date','Rate','Units']
    cur_data = []
    for currency_name in currency_list:
        cur_csv = pd.read_csv('currency/'+ currency_name +'_days_table.csv',usecols=currency_columns, error_bad_lines=False)
        cur_csv[currency_name] = cur_csv['Rate']/cur_csv['Units']
        if len(cur_data) == 0 :
            cur_data = cur_csv[{'Date',currency_name}]
        else:
            cur_data = cur_data.join(cur_csv[{'Date',currency_name}].set_index('Date'), on='Date')
    return cur_data

def join_covid_currency():
    cur_columns = ['Date','USD','EUR','CHF','CNY','SEK','UAH','JPY']
    cur_data = pd.read_csv('all_currency_by_date.csv',usecols=cur_columns)

    days_covid_data = pd.read_csv('days_table_COVID19.csv')
    days_covid_data = days_covid_data.rename(columns={'Last Update': 'Date'})
    days_covid_data = days_covid_data.join(cur_data.set_index('Date'), on='Date')
    days_covid_data.to_csv('covid_currency_by_dates.csv')

    countries_covid_data = pd.read_csv('countries_days_table_COVID19.csv')
    countries_covid_data = countries_covid_data.rename(columns={'Last Update': 'Date'})
    countries_covid_data = countries_covid_data.join(cur_data.set_index('Date'), on='Date')
    countries_covid_data.to_csv('covid_currency_by_countries.csv')


if __name__ == "__main__":
    #covid
    csv_folder = 'full_stright_table_COVID19.csv'
    important_columns=['Country/Region','Last Update','Confirmed','Deaths','Recovered']
    csv_data = get_data_csv(csv_folder,important_columns)
    dims = ['Country/Region','Last Update']
    measures = {'Confirmed': 'sum','Deaths': 'sum','Recovered': 'sum'}
    aggr_data = transform_data(csv_data,dims,measures)
    aggr_data.to_csv('countries_days_table_COVID19.csv')
    dims = ['Last Update']
    aggr_data = transform_data(csv_data,dims,measures)
    aggr_data.to_csv('days_table_COVID19.csv')
    #covid add exchange
    cur_data = get_currency_by_date()
    cur_data.to_csv('all_currency_by_date.csv')
    join_covid_currency()
