import pandas as pd

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
    return raw_data

if __name__ == "__main__":
    link_for_urls = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports'
    csv_links = get_data_links(link_for_urls)
    raw_data = extract_csv(csv_links)
    raw_data.to_csv('full_stright_table_COVID19.csv')