# %% Admin
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pmaw import PushshiftAPI
import praw
import os


# %% PRAW Connection
def redditconnect_PRAW():

    # Cloud Connection Information
    default_credential = DefaultAzureCredential()
    secret_client = SecretClient(
        vault_url="https://keyvaultforquant.vault.azure.net/",
        credential=default_credential
    )
    temp_prawcred_cliendid    = secret_client.get_secret(name="RedditConnect-clientid")
    temp_prawcred_clientsecret= secret_client.get_secret(name="RedditConnect-clientsecret")
    temp_prawcred_username    = secret_client.get_secret(name="RedditConnect-username")
    temp_prawcred_passwords   = secret_client.get_secret(name="RedditConnect-password")

    # Praw Object
    prawreddit_object= praw.Reddit(client_id=temp_prawcred_cliendid.value,
                                    client_secret=temp_prawcred_clientsecret.value,
                                    username=temp_prawcred_username.value, password=temp_prawcred_passwords.value,
                                    user_agent="prawtutorialv1")

    return prawreddit_object
# conn_reddit_object= connect_to_reddit()

# %% PMAW Connection
def redditconnect_PMAW():
    # Pmaw Object
    temp_threadcount = os.cpu_count()
    api = PushshiftAPI(num_workers=temp_threadcount * 5)

    return api



