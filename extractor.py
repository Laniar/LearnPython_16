import pandas as pd
import requests

def get_data_links(link_for_urls):
    csv_links = []
    link_table = pd.read_html(link_for_urls)
    for link_pd in link_table[0].Name:
        if link_pd[-3:] == 'csv' :
            csv_links.append(link_pd)
    return csv_links

def extract_csv(csv_links):
    url_template = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
    raw_data_list = []
    for url_covid in csv_links:
        url_covid_csv = url_template + url_covid
        df_covid_raw = pd.read_csv(url_covid_csv, error_bad_lines=False)
        raw_data_list.append(df_covid_raw)
    raw_data = pd.concat(raw_data_list)
    #clear dataset
    raw_data['Last Update'].update(raw_data['Last_Update'])
    raw_data['Province/State'].update(raw_data['Province_State'])
    raw_data['Country/Region'].update(raw_data['Country_Region'])
    raw_data['Latitude'].update(raw_data['Lat'])
    raw_data['Longitude'].update(raw_data['Long_'])
    raw_data['Confirmed'].fillna(0)
    raw_data['Deaths'].fillna(0)
    raw_data['Recovered'].fillna(0)
    raw_data['Last Update'] = pd.to_datetime(raw_data['Last Update'])
    raw_data['Last_Update'] = pd.to_datetime(raw_data['Last_Update'])
    return raw_data

def get_exchange_rate(currency,currency_id,date_from,date_to):
    request_params = {'UniDbQuery.Posted' : 'True', 'UniDbQuery.mode' : '1', 'UniDbQuery.date_req1' : '', 'UniDbQuery.date_req2' : '', 'UniDbQuery.VAL_NM_RQ' : currency_id, 'UniDbQuery.From' : date_from, 'UniDbQuery.To' : date_to}
    request_url='https://cbr.ru/currency_base/dynamics/'
    url_rates = requests.get(request_url,request_params).url
    cur_data = pd.read_html(url_rates,header=1,thousands=',')[0]
    cur_data['Currency'] = currency
    cur_data = cur_data.rename(columns={cur_data.columns[0]: 'Date', cur_data.columns[1]: 'Units', cur_data.columns[2]: 'Rate'})
    cur_data['Date'] = pd.to_datetime(cur_data['Date'])
    cur_data['Rate'] = cur_data['Rate'].div(10000)
    file_name = 'currency/' + currency + '_' + 'days_table' + '.csv'
    cur_data.to_csv(file_name)
    #return cur_data

if __name__ == "__main__":
    #covid section
    link_for_urls = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports'
    csv_links = get_data_links(link_for_urls)
    raw_data = extract_csv(csv_links)
    raw_data.to_csv('full_stright_table_COVID19.csv')
    #exchange section
    get_exchange_rate('USD','R01235','22.01.2020','18.04.2021')
    get_exchange_rate('EUR','R01239','22.01.2020','18.04.2021')
    get_exchange_rate('CHF','R01775','22.01.2020','18.04.2021')
    get_exchange_rate('CNY','R01375','22.01.2020','18.04.2021')
    get_exchange_rate('SEK','R01770','22.01.2020','18.04.2021')    
    get_exchange_rate('UAH','R01720','22.01.2020','18.04.2021') 
    get_exchange_rate('JPY','R01820','22.01.2020','18.04.2021')