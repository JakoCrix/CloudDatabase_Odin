# %% Admin
from datetime import datetime
import pandas as pd
import datetime
# from Helper.Connections_Database import *
# from Odin_Interactions.Extraction.ScrapeComments import *

def IP_tickerinfo(RawTickers_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod(); RawTickers_Df= scrape_all
    stockinfo= RawTickers_Df.copy()

    # Identifying New Tickers
    CurrentTable = pd.read_sql_query("SELECT name, ticker FROM stock_info", conn_Object)
    stockinfo["DropMe"]= stockinfo["symbol"].isin(CurrentTable["ticker"].tolist())
    stockinfo2 = stockinfo[stockinfo.DropMe==False].copy()
    stockinfo2.drop("DropMe", axis=1, inplace=True)

    # Processing into odin format
    stockinfo2["idticker"] = range(1, len(stockinfo2)+1)
    stockinfo2.loc[stockinfo2["ipoyear"]=="", "ipoyear"]="0"
    stockinfo2 = stockinfo2.astype({"ipoyear":int})

    # Finalizing and Returning
    processed_tickerinfo= stockinfo2[["idticker", "name", "symbol", "country", "ipoyear", "sector", "industry"]]
    processed_tickerinfo.columns = ["idstock", "name", "ticker", "country", "ipoyear", "sector", "industry"]

    return processed_tickerinfo


def IP_financialtracking(RawTickers_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod(); RawTickers_Df= scrape_all
    RawTickers_Df = RawTickers_Df[["name", "symbol", "marketCap", "volume"]].copy()

    # Merging tickerids
    stockinfo_id = pd.read_sql_query("SELECT idstock, name, ticker FROM stock_info", conn_Object)
    financialtracking = pd.merge(left=RawTickers_Df, right=stockinfo_id, how="left", left_on=["name", "symbol"],
                                 right_on=["name", "ticker"])
    financialtracking['idstock'] = financialtracking['idstock'].astype('Int64')

    # Processing into odin format
    financialtracking2 = financialtracking.copy()
    financialtracking2["lastfetched"] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    financialtracking2.loc[financialtracking2["marketCap"]=="", "marketCap"]="0"
    financialtracking2["marketCap"] = financialtracking2.marketCap.str.replace(r".00$","", regex= True)
    financialtracking2 = financialtracking2.astype({"marketCap":float})
    financialtracking2["marketCap"] = financialtracking2["marketCap"]/1000000
    financialtracking2 = financialtracking2.astype({"marketCap":int})

    financialtracking2.loc[financialtracking2["volume"]=="", "volume"]="0"
    financialtracking2 = financialtracking2.astype({"volume":int})

    # Finalizing and Returning
    processed_financialtracking = financialtracking2[["idstock", "lastfetched", "marketCap", "volume"]]
    processed_financialtracking.columns = ["idstock", "lastfetched", "marketcap", "volume"]

    return processed_financialtracking

