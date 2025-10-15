# RedditFlow: Reddit Data Ingestor to MongoDB

RedditFlow is a Python tool designed to pull data from Reddit and save it into a MongoDB database.

It uses an efficient process to fetch data quickly without getting blocked by Reddit's limits.

---

## 1. Project Goal

The purpose of this tool is to create a reliable process (ETL: Extract, Transform, Load) to archive Reddit posts and comments in a MongoDB collection.

---

## 2. Setup and Installation

### Prerequisites

You need Python 3.8+ and a MongoDB server running (local or remote).

### Steps

1.  **Create Environment:**
    ```bash
    conda create --name reddit-etl python=3.11
    conda activate reddit-etl
    ```

2.  **Install Libraries:**
    ```bash
    # Install the necessary libraries
    pip install praw pymongo python-dotenv
    # NOTE: We use the faster, asynchronous PRAW library.
    ```

3.  **Set Up Credentials:**
    Create a file named **`.env`** in the project's main folder and add your access keys and database address:

    ```ini
    # .env file content
    REDDIT_CLIENT_ID="YOUR_REDDIT_CLIENT_ID"
    REDDIT_CLIENT_SECRET="YOUR_REDDIT_CLIENT_SECRET"
    REDDIT_USER_AGENT="RedditFlowETL (by /u/YourUsername)"
    MONGO_URI="mongodb://localhost:27017/" 
    ```

4.  **Set Configuration:**
    Edit the file `config/config.py` to choose which subreddits and how many posts you want to fetch.

---

## 3. How to Run

Execute the main script from your terminal:

```bash
python main.py
