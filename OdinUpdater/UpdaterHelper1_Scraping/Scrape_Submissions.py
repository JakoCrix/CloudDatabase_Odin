# %% Admin
import pandas as pd
import time

def ScrapeReddit_SubmissionviaSubreddit(Subreddit_Name, conn_reddit_object, MinimumComments=30):
    # Subreddit_Name="stocks"; conn_reddit_object= connect_to_reddit(); MinimumComments=25

    # Scraping-Connection
    subreddit = conn_reddit_object.subreddit(Subreddit_Name)
    AllPosts = subreddit.new(limit=1000)  # limit to 1000 submissions

    # Scraping- Scraping Process
    tempself_SubredditName = subreddit.display_name
    tempself_SubredditID = subreddit.id
    tempself_CurrentUTC = int(time.time())

    temp_SubmissionCount = 0
    temp_SubmissionHaveContent = 0
    temp_SubmissionLackContent = 0
    Temp_data = []

    for submission in AllPosts:
        # submission= conn_reddit_object.submission("j2gc7k")
        temp_SubmissionCount += 1

        try:
            if submission.num_comments >= MinimumComments:
                Temp_data.append([tempself_SubredditID, tempself_SubredditName,
                                  submission.id, submission.title, submission.selftext, submission.url, submission.stickied, submission.created_utc,
                                  tempself_CurrentUTC, submission.num_comments, submission.score, submission.upvote_ratio, submission.is_original_content
                                  ])
                # Tracking
                temp_SubmissionHaveContent += 1
                if temp_SubmissionCount % 100 == 0:
                    print("In subreddit {} of {} comments: {} has content and {} lacks content".format(Subreddit_Name,
                                                                                                       temp_SubmissionCount,
                                                                                                       temp_SubmissionHaveContent,
                                                                                                       temp_SubmissionLackContent))
            else:
                # Tracking
                temp_SubmissionLackContent += 1
                if temp_SubmissionCount % 100 == 0:
                    print("In subreddit {} of {} comments: {} has content and {} lacks content".format(Subreddit_Name,
                                                                                                       temp_SubmissionCount,
                                                                                                       temp_SubmissionHaveContent,
                                                                                                       temp_SubmissionLackContent))
        except:
            pass


    df = pd.DataFrame(Temp_data, columns=['Subreddit_ID', 'Subreddit_Name',
                                          'Submission_ID','Submission_Title','Submission_Selftext','Submission_URL','Submission_Stickied','Submission_UTCCreationTime',
                                          'Submission_UTCFetchTime', 'Submission_NumComments', "Submission_Score",'Submission_UpvoteRatio', 'Submission_OriginalContent'])

    return df

