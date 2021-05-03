# Admin
import pandas as pd
import datetime as datetime
from dateutil import tz
import re

from Helper.Connections_Scraping import redditconnect_PMAW

# %% Scraper 1- Via ID
def PmawScrapeSubmissions_viaSubmissionID(redditconnect_pmawobj, submissionID_List):
    # redditconnect_pmawobj= redditconnect_PMAW(); submissionID_List = PmawScrapeComments_viaday(redditconnect_pmawobj,  "2021/04/26", "stocks")["rId_submission"].unique()

    # Scraping Process
    print("Extraction Process: Starting extraction")
    timestart = datetime.datetime.now()
    Temp_ColumnsStr = ["author", "author_fullname", "created_utc", "full_link", "id", "retrieved_on", "selftext", "subreddit", "title"]
    submissions_raw = redditconnect_pmawobj.search_submissions(ids=submissionID_List, fields=Temp_ColumnsStr)
    print("Extraction Process: Completing extraction, time taken in seconds: {}".format((datetime.datetime.now() - timestart).seconds))

    submissions0 = pd.DataFrame(submissions_raw)
    submissions1 = submissions0[Temp_ColumnsStr].copy()
    submissions1.columns = ["post_author", "rId_author", "post_createdutc", "post_fulllink", "rId_submission", "post_retrieved_on",
                            "post_selftext", "post_subreddit", "post_title"]
    # Processing Output
    submissions2 = submissions1.copy()
    submissions2["rId_author"] = submissions2['rId_author'].apply(lambda x: re.sub("^t._", "", str(x)))
    submissions2["created_utc"] = submissions2["created_utc"].apply(datetime.datetime.utcfromtimestamp)
    submissions2["retrieved_on"] = submissions2["retrieved_on"].apply(datetime.datetime.utcfromtimestamp)

    # Returning
    submissions_Final= submissions2.copy()

    return submissions_Final


# %% Scraper 2- Via Dates
def PmawScrapeSubmissions_viaday(redditconnect_pmawobj, dateofinterest_str, subreddit_str):
    # redditconnect_pmawobj = redditconnect_PMAW(); dateofinterest_str = "2021/04/26"; subreddit_str = "stocks"
    print("__"*20)

    # Date Specifier
    EarliestSubmission = datetime.datetime.strptime(dateofinterest_str, "%Y/%m/%d").date()
    EarliestSubmission2 = datetime.datetime(EarliestSubmission.year, EarliestSubmission.month, EarliestSubmission.day, tzinfo=tz.tzutc())
    EarliestSubmission3 = int(EarliestSubmission2.timestamp())
    LatestSubmission = EarliestSubmission + datetime.timedelta(days=14)
    LatestSubmission2 = datetime.datetime(LatestSubmission.year, LatestSubmission.month, LatestSubmission.day, tzinfo=tz.tzutc())
    LatestSubmission3 = int(LatestSubmission2.timestamp())

    # Scraping Process
    print("_Scraping Submissions- Process Started")
    timestart = datetime.datetime.now()
    Temp_ColumnsStr = ["author", "author_fullname", "created_utc", "full_link", "id", "retrieved_on", "selftext", "subreddit", "title"]
    submissions_raw = redditconnect_pmawobj.search_submissions(subreddit=subreddit_str, fields=Temp_ColumnsStr, after=EarliestSubmission3, before=LatestSubmission3)
    print("_Scraping Submissions- Completing extraction, time taken: {}".format((datetime.datetime.now() - timestart).seconds))

    submissions0 = pd.DataFrame(submissions_raw)
    submissions1 = submissions0[Temp_ColumnsStr].copy()
    submissions1.columns = ["post_author", "rId_author", "post_createdutc", "post_fulllink", "rId_submission", "post_retrievedon",
                            "post_selftext", "post_subreddit", "post_title"]

    # Processing Output
    submissions2 = submissions1.copy()
    submissions2["rId_author"] = submissions2['rId_author'].apply(lambda x: re.sub("^t._", "", str(x)))
    submissions2["post_createdutc"] = submissions2["post_createdutc"].apply(datetime.datetime.utcfromtimestamp)
    submissions2["post_retrievedon"] = submissions2["post_retrievedon"].apply(datetime.datetime.utcfromtimestamp)

    # Returning
    submissions_Final= submissions2.copy()

    return submissions_Final




