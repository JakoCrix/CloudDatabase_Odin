CREATE VIEW [submission_summary] AS
SELECT SI.idsubmission, SI.title, SI.url, SI.createdatetime, SR.subreddit,
       ST.lastfetched_max, ST.numcomments_max
FROM submission_info SI
LEFT JOIN (select idsubmission, idsubreddit, 
                  max(numcomments) as numcomments_max, max(lastfetched) as lastfetched_max
            from submissiontracking group by idsubmission, idsubreddit) ST
    ON ST.idsubmission= SI.idsubmission
LEFT JOIN (select idsubreddit, title AS subreddit FROM subreddit_info) SR
    ON SR.idsubreddit= ST.idsubreddit
WHERE lastfetched_max IS NOT NULL