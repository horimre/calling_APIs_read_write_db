import json
import requests
from requests.exceptions import HTTPError
import logging
import pandas as pd
import sqlalchemy
import ApiKeys
import DbConf


class EmptyResultException(Exception):
    def __init__(self, msg, val):
        self.msg = msg
        self.val = val


def execute_request(url: str, params: dict, topic: str, company: str) -> json:
    try:
        response = requests.get(url, params=params)

        response.raise_for_status()

        response_json = response.json()

        # dump response_json to {topic}_{company}.json file
        with open(f'{topic}_{company}.json', 'w') as file:
            json.dump(response_json, file, indent=4)

    except HTTPError as http_err:
        # logging.error(f"Request failed. HTTP error: {http_err} Message: {response.text}")
        raise
    except Exception as e:
        # logging.error(f"Other Error: {e}", exc_info=True)
        raise
    else:
        logging.info(f"Request finished successfully. "
                     f"Status code: {response.status_code} "
                     f"Reason: {response.reason}")
        return response_json


def get_stock_prices(stock: str) -> json:
    # request url
    url = "https://www.alphavantage.co/query"

    # request params
    params = {"function": "TIME_SERIES_DAILY",
              "symbol": stock,
              "apikey": ApiKeys.api_keys["alphavantage"]}

    # make the request
    logging.info("get the stock prices")
    try:
        response_json = execute_request(url, params, "stock", stock)
        return response_json
    except Exception as e:
        # logging.error(f"Error: {e}", exc_info=True)
        raise


def process_stock_prices(stock_name: str, company_name: str, stock_json: json) -> pd.DataFrame:
    # process json
    try:

        df = pd.DataFrame(stock_json["Time Series (Daily)"])
        # print(df.head())

        cols = ["Company", "Date", "Positive change", "Title of most recent article"]
        result_df = pd.DataFrame(columns=cols)

        # we are only interested in the closing value of the last three days
        df_last_3_days_closing = df.iloc[3, :3]

        # set datatype to float
        df_last_3_days_closing = df_last_3_days_closing.astype(float)
        df_last_3_days_closing = df_last_3_days_closing.sort_index()
        # print(df_last_3_days_closing)

        for index, stock_price in enumerate(df_last_3_days_closing):
            # print(index, value)

            one_percent = stock_price * 0.01
            # print("one percent: ", one_percent)

            # print(len(df_last_3_days_closing))
            if index != len(df_last_3_days_closing) - 1:

                positive_change = abs(df_last_3_days_closing[index] - df_last_3_days_closing[index + 1])

                if positive_change >= one_percent:
                    positive_change = round(positive_change, 2)
                    change_date = \
                        df_last_3_days_closing[df_last_3_days_closing == df_last_3_days_closing[index + 1]].index[0]

                    # print(f'{change_date}: at least 1% change. '
                    #      f'Positive change: {positive_change}')

                    news_response = get_news(company_name, change_date)
                    article_title = process_news(news_response)

                    print(company_name, change_date, positive_change, article_title)
                    df_company = pd.DataFrame([[company_name, change_date, positive_change, article_title]],
                                              columns=column_names)
                    result_df = pd.concat([result_df, df_company], ignore_index=True)
                    # print(result_df)

        return result_df

    except KeyError as e:
        # logging.error(f"{company_name} - Stock not found!")
        raise EmptyResultException('Stock not found!', stock_name)


def get_news(company_name: str, from_date: str) -> json:
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": company_name,
        "from": from_date,
        "sortBy": "popularity",
        "apiKey": ApiKeys.api_keys["newsapi"]
    }

    logging.info("get articles")
    try:
        response_json = execute_request(url, params, "news", company_name)
        return response_json
    except Exception as e:
        # logging.error(f"Error: {e}", exc_info=True)
        raise


def process_news(news_json: json) -> str:
    # process json
    try:
        df = pd.DataFrame(news_json["articles"])
        df = df.loc[0, "title"]
        return df
    except KeyError as e:
        logging.error("No articles found!")
        return "No articles found!"


def load_to_db(engine: sqlalchemy.engine.base.Engine, df: pd.DataFrame):
    try:
        df.to_sql(DbConf.db_conf["table_name"], engine, if_exists='append', index=False)
    except Exception as e:
        # logging.error(f"Error: {e}", exc_info=True)
        raise


def read_from_db(engine: sqlalchemy.engine.base.Engine):
    try:
        df = pd.read_sql(f'SELECT * FROM {DbConf.db_conf["table_name"]} WHERE Company = "Oracle"', engine)
        df.to_csv('out_db_result.csv', sep=',', index=False)
    except Exception as e:
        # logging.error(f"Error: {e}", exc_info=True)
        raise


# set log level
logging.basicConfig(level=logging.WARNING)

# set sql engine
sql_engine = sqlalchemy.create_engine(f'mysql+pymysql://{DbConf.db_conf["user"]}:{DbConf.db_conf["password"]}'
                                      f'@localhost:{DbConf.db_conf["port"]}/{DbConf.db_conf["db"]}')

companies = {"IBM": "Ibm", "TSLA": "Tesla", "ORCL": "Oracle", "XYZABS": "blablabla", "AMZN": "Amazon"}
# companies = {"IBM": "Ibm"}

column_names = ["Company", "Date", "Positive change", "Title of most recent article"]
res_df = pd.DataFrame(columns=column_names)

for key, value in companies.items():
    # print(key, value)

    try:
        stock_json_response = get_stock_prices(key)
        stock_df = process_stock_prices(key, value, stock_json_response)

        res_df = pd.concat([res_df, stock_df], ignore_index=True)
        # print(res_df)
    except Exception as ex:
        logging.error(ex)
        # print(f"Error: {ex}")

res_df.to_csv('out_result.csv', sep=',', index=False)

try:
    load_to_db(sql_engine, res_df)
    read_from_db(sql_engine)
except Exception as ex:
    logging.error(ex)
