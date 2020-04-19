import pandas as pd

def get_data_csv(csv_folder,important_columns):  
    csv_data = pd.read_csv(csv_folder,usecols=important_columns)
    return csv_data

def transform_data(csv_data,dims,measures):
    csv_data['Last Update'] = pd.to_datetime(csv_data['Last Update']).dt.date
    aggr_data = csv_data.groupby(dims).agg(measures)
    return aggr_data

if __name__ == "__main__":
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