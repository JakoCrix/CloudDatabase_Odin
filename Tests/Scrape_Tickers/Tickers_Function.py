# %% Admin
import pandas as pd
import requests
import io
import re

# %% Tickers Extraction
def NasdaqTickers_Webscrape(Exchange_OfInterest= "All"):
    # Exchange_OfInterest= "All"

    # Admin
    Temp_getheaders = {'authority': 'old.nasdaq.com', 'upgrade-insecure-requests': '1',
                       'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                       'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                       'sec-fetch-site': 'cross-site','sec-fetch-mode': 'navigate', 'sec-fetch-user': '?1', 'sec-fetch-dest': 'document', 'accept-language': 'en-US,en;q=0.9',
                       'cookie': 'AKA_A2=A; NSC_W.TJUFEFGFOEFS.OBTEBR.443=ffffffffc3a0f70e45525d5f4f58455e445a4a42378b'}

    # Extraction
    UrlLinks= {"Nasdaq": "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download",
               "Amex":   "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download",
               "NYSE":   "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download"}

    if   Exchange_OfInterest != "All" and Exchange_OfInterest not in list(UrlLinks.keys()):
        print("Invalid Exchange listed, please insert: {}".format(", ".join(list(UrlLinks.keys()))))
        pass

    elif Exchange_OfInterest != "All" and Exchange_OfInterest in list(UrlLinks.keys()) :
        print("Valid Exchange listed")
        print("- extracting {} tickers".format(Exchange_OfInterest))
        URLRaw = requests.get(UrlLinks[Exchange_OfInterest], headers=Temp_getheaders)
        URLRaw2 = io.StringIO(URLRaw.text)
        URLRaw_Final = pd.read_csv(URLRaw2, sep=",")

    else:
        URLRaw_Final = pd.DataFrame()
        print("Extracting from {}".format(", ".join(list(UrlLinks.keys()))))

        for Exchange in list(UrlLinks.keys()):
            print("- extracting {} tickers".format(Exchange))
            URLRaw = requests.get(UrlLinks[Exchange], headers=Temp_getheaders)
            URLRaw2 = io.StringIO(URLRaw.text)
            URLRaw_Temp = pd.read_csv(URLRaw2, sep=",")

            URLRaw_Final= URLRaw_Final.append(URLRaw_Temp)

    # Returning
    Tickers = URLRaw_Final[~URLRaw_Final['Symbol'].str.contains("\.|\^")]. \
        drop("Unnamed: 8", 1). \
        drop_duplicates(keep=False)
    Tickers_Final =Tickers. \
        sort_values("Symbol", ignore_index=True).\
        copy()
    Tickers_Final.columns=["Symbol", "Name", "LastSale", "MarketCap", "IPOyear", "Sector", "Industry", "SymbolURL"]

    return Tickers_Final

def NasdaqTickers_Process1(NasdaqTickers_Df, StopwordLimit_Int=25):
    # Admin
    # NasdaqTickers_Df = NasdaqTickers_Webscrape()

    # %% Process1- Handling ETF's
    NasdaqTickers1 = NasdaqTickers_Df.copy()
    NasdaqTickers1.loc[NasdaqTickers1["Sector"].isna(), "Sector"] = "ETF"

    # %% Process2- Removing Punctuations
    NasdaqTickers1["Name"] = [re.sub(r"[^\w\s]", "", word) for word in NasdaqTickers1["Name"].tolist()]

    # %% Process3- Removing Unnecessary names
    # _Identifying Unnecessary names
    Allwords = [word for line in NasdaqTickers1["Name"].tolist() for word in line.split()]
    WordDict = {}
    for word in Allwords:
        WordDict[word] = WordDict.setdefault(word, 0) + 1
    UnnecessaryWords = [k for (k, v) in WordDict.items() if v > StopwordLimit_Int]
    UnnecessaryWords = [word for word in UnnecessaryWords if word not in ""]

    # Manual Checks
    #Deleteme= pd.DataFrame({"Wordset":list(WordDict.keys()),"Counter":list(WordDict.values())})
    #Deleteme=Deleteme.sort_values("Counter",ascending=False).reset_index(drop=True)
    #Deleteme[75:125]

    def Temp_RemoveStopwords(Name_Column, StopWords_list):
        # Function takes in _ and _
        Temp_List = Name_Column.split(" ")
        Temp_ListAmmended = [elem for elem in Temp_List if elem not in StopWords_list]
        Temp_Joined = " ".join(Temp_ListAmmended)
        return Temp_Joined

    NasdaqTickers1["Name_Short"] = NasdaqTickers1.apply(lambda row: Temp_RemoveStopwords(row["Name"], UnnecessaryWords),
                                                        axis=1)

    # Process4- Including all possible combinations
    NasdaqTickers2 = NasdaqTickers1.copy()
    NasdaqTickers2["Combination1"] = NasdaqTickers2["Symbol"]
    NasdaqTickers2["Combination2"] = NasdaqTickers2["Name_Short"].str.rstrip(" ")
    NasdaqTickers2["Combination3"] = NasdaqTickers2["Combination2"].str.lower()

    AdditionalNames = pd.read_csv("Data\\Tickers\\TickerNames_Additional.csv")
    AdditionalNames["Combination4"] = AdditionalNames["Combination_Names"].str.replace(", ",",")
    AdditionalNames = AdditionalNames.drop("Combination_Names", 1)
    NasdaqTickers2 = pd.merge(left=NasdaqTickers2, right=AdditionalNames, how="left", on="Symbol")

    NasdaqTickers2["Combinations"] = NasdaqTickers2[["Combination1", "Combination2", "Combination3", "Combination4"]]. \
        fillna("").agg('|'.join, axis=1). \
        str.rstrip("|")

    NasdaqTickers2["Combinations"] = NasdaqTickers2["Combinations"].str.replace(",", "|")

    # Returning
    NasdaqTickers3 = NasdaqTickers2[["Symbol", "Sector", "Industry", "Combinations"]].copy()

    return NasdaqTickers3

