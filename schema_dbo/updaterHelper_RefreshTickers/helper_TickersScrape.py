# Admin
import pandas as pd
import requests
import json

# Scraping Function
def params(exchange='NYSE', regions=None, countries=None, sectors=None, analystRatings=None):
    # Hard-coding information
    _EXCHANGE_LIST = ['nyse', 'nasdaq', 'amex']
    _SECTORS_LIST = set(['Consumer Non-Durables', 'Capital Goods', 'Health Care', 'Energy', 'Technology', 'Basic Industries', 'Finance', 'Consumer Services', 'Public Utilities', 'Miscellaneous', 'Consumer Durables', 'Transportation'])
    _ANALYST_RATINGS_LIST = set(['Strong Buy', 'Hold', 'Buy', 'Sell', 'Strong Sell'])
    _REGIONS_LIST = set(['AFRICA', 'EUROPE', 'ASIA', 'AUSTRALIA+AND+SOUTH+PACIFIC', 'CARIBBEAN', 'SOUTH+AMERICA', 'MIDDLE+EAST','NORTH+AMERICA'])
    _COUNTRIES_LIST = set(['Argentina', 'Armenia', 'Australia', 'Austria', 'Belgium', 'Bermuda', 'Brazil', 'Canada', 'Cayman Islands','Chile', 'Colombia','Costa Rica', 'Curacao', 'Cyprus', 'Denmark', 'Finland', 'France', 'Germany', 'Greece', 'Guernsey',
                           'Hong Kong', 'India', 'Indonesia', 'Ireland','Isle of Man', 'Israel', 'Italy', 'Japan', 'Jersey', 'Luxembourg', 'Macau', 'Mexico', 'Monaco', 'Netherlands',
                           'Norway', 'Panama', 'Peru','Philippines', 'Puerto Rico', 'Russia', 'Singapore', 'South Africa', 'South Korea', 'Spain', 'Sweden','Switzerland', 'Taiwan', 'Turkey','United Kingdom', 'United States'])

    # Params Creation
    params = (('exchange', exchange), ('download', 'true'), ('tableonly', 'true'))

    if regions is not None:
        if isinstance(regions, str):
            regions = [regions]
        if not _REGIONS_LIST.issuperset(set(regions)):
            raise ValueError('Some regions included are invalid')
        params = params + (('region', '|'.join(regions)),)

    if sectors is not None:
        if isinstance(sectors, str):
            sectors = [sectors]
        if not _SECTORS_LIST.issuperset(set(sectors)):
            raise ValueError('Some sectors included are invalid')
        params = params + (('sector', '|'.join(sectors)),)

    if countries is not None:
        if isinstance(countries, str):
            countries = [countries]
        if not _COUNTRIES_LIST.issuperset(set(countries)):
            raise ValueError('Some countries included are invalid')
        params = params + (('country', '|'.join(countries)),)

    if analystRatings is not None:
        if isinstance(analystRatings, str):
            analystRatings = [analystRatings]
        if not _ANALYST_RATINGS_LIST.issuperset(set(analystRatings)):
            raise ValueError('Some ratings included are invalid')
        params = params + (('recommendation', '|'.join(analystRatings)),)

    return params

def get_tickers(exchange, regions=None,sectors=None,countries=None,analystRatings=None):
    # Getting around Anti-Scraping Policies
    headers = {
        'authority': 'nasdaq.com',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://github.com/shilewenuw/get_all_tickers/issues/2',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'AKA_A2=A; NSC_W.TJUFEFGFOEFS.OBTEBR.443=ffffffffc3a0f70e45525d5f4f58455e445a4a42378b',
    }

    response = requests.get('https://api.nasdaq.com/api/screener/stocks',
                            headers=headers,
                            params=params(exchange=exchange,
                                          regions=regions,
                                          sectors=sectors,
                                          countries=countries,
                                          analystRatings=analystRatings))


    text_data = response.text
    json_dict = json.loads(text_data)

    # Handling Errors
    if json_dict['data']['headers'] is None:
        return pd.DataFrame()

    columns = list(json_dict['data']['headers'].keys())
    df = pd.DataFrame(json_dict['data']['rows'], columns=columns)

    return(df)

