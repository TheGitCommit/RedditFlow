import os
from dotenv import load_dotenv

load_dotenv()

# --- REDDIT API CONFIG ---
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "YOU_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "YOU_CLIENT_SECRET")
REDDIT_USER_AGENT = "RedditETL by /u/user"

# --- MONGODB CONFIG ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DATABASE = "reddit_data_lake"
POSTS_COLLECTION = "posts"
COMMENTS_COLLECTION = "comments"

# --- ETL PARAMETERS ---
SUBREDDITS_TO_SCRAPE = ["computerscience"]
SORTING_METHOD = "hot"
POST_LIMIT = 100  # Posts to fetch per run

# --- COMMENT EXTRACTION CONFIG ---
FETCH_COMMENTS = True  # Set to False to skip comment extraction
COMMENT_REPLACE_MORE_LIMIT = None  # None = fetch all, or set int (e.g., 5) to limit "MoreComments" expansion
MAX_COMMENT_DEPTH = None  # None = fetch all depths, or set int (e.g., 5) to limit nesting level

# --- RATE LIMITING ---
DELAY_BETWEEN_SUBREDDITS = 2  # Seconds to wait between subreddit fetches
DELAY_BETWEEN_POSTS = 1  # Seconds to wait between fetching comments for each post