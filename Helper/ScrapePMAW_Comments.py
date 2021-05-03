# Admin
import pandas as pd
import datetime as datetime
from dateutil import tz
import re

from Helper.Connections_Scraping import redditconnect_PMAW
# %% Scraping Functions
def PmawScrapeComments_viaday(redditconnect_pmawobj, dateofinterest_str, subreddit_str):
    # redditconnect_pmawobj = redditconnect_PMAW(); dateofinterest_str = "2021/04/26"; temp_subreddit = "stocks"

    # Date Specifier
    EarliestComment = datetime.datetime.strptime(dateofinterest_str, "%Y/%m/%d").date()
    EarliestComment2 = datetime.datetime(EarliestComment.year, EarliestComment.month, EarliestComment.day,
                                         tzinfo=tz.tzutc())
    EarliestComment3 = int(EarliestComment2.timestamp())

    LatestComment = EarliestComment + datetime.timedelta(days=1)
    LatestComment2 = datetime.datetime(LatestComment.year, LatestComment.month, LatestComment.day, tzinfo=tz.tzutc())
    LatestComment3 = int(LatestComment2.timestamp())

    # Scraping
    print("Extraction Process: Starting extraction")
    timestart = datetime.datetime.now()
    Temp_ColumnsStr = ["subreddit_id", "link_id", "permalink", "author", "author_fullname", "id", "parent_id","body", "created_utc", "is_submitter", "retrieved_on", "score", "stickied"]
    comments = redditconnect_pmawobj.search_comments(subreddit=subreddit_str, fields=Temp_ColumnsStr,after=EarliestComment3, before=LatestComment3)
    print("Extraction Process: Completing extraction, time taken in seconds: {}".format((datetime.datetime.now() - timestart).seconds))
    comments_raw = pd.DataFrame(comments)

    comments1 = comments_raw[Temp_ColumnsStr].copy()
    comments1.columns = ["rId_subreddit", "rId_submission", "submissionURL", "author", "rId_author", "rId_id",
                         "rId_parentid",
                         "body", "created_utc", "is_submitter", "retrieved_on", "score", "stickied"]

    # Processing Output
    comments2 = comments1.copy()
    comments2["rId_subreddit"] = comments2['rId_subreddit'].apply(lambda x: re.sub("^t._", "", str(x)))
    comments2["rId_submission"] = comments2['rId_submission'].apply(lambda x: re.sub("^t._", "", str(x)))
    comments2["rId_author"] = comments2['rId_author'].apply(lambda x: re.sub("^t._", "", str(x)))
    comments2["rId_parentid"] = comments2['rId_parentid'].apply(lambda x: re.sub("^t._", "", str(x)))

    comments2["created_utc"] = comments2["created_utc"].apply(datetime.datetime.utcfromtimestamp)
    comments2["retrieved_on"] = comments2["retrieved_on"].apply(datetime.datetime.utcfromtimestamp)

    # Removing Deleted Rows
    comments3 = comments2.copy()

    # Returning
    comments_Final= comments3.copy()

    return comments_Final





