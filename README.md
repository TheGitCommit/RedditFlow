# RedditFlow: Reddit Data Ingestor to MongoDB

**RedditFlow** is a Python tool designed to pull data from Reddit and save it into a MongoDB database.

It uses an efficient, asynchronous process to fetch data quickly while complying with Reddit's rate limits.

---

## 1. Project Goal

The purpose of this tool is to create a reliable process (ETL: **E**xtract, **T**ransform, **L**oad) to archive Reddit posts and comments in a MongoDB collection. The data is stored in separate collections using an update-or-insert method to prevent duplicates.

---

## 2. Setup and Installation

### Prerequisites

You need the **Conda** package manager (usually via Anaconda or Miniconda) and a **MongoDB server** (local or remote).

### Steps

1.  **Create and Activate Environment (using `environment.yml`):**
    This command will create the environment named `reddit-etl` and install all necessary dependencies (Python, `asyncpraw`, `pymongo`, etc.) as specified in the `environment.yml` file.
    ```bash
    conda env create -f environment.yml
    conda activate reddit-etl
    ```

2.  **Configure Credentials (`.env` file):**
    Create a file named **`.env`** in your project's root folder to securely store your API keys and MongoDB address.
    ```ini
    # .env file content
    REDDIT_CLIENT_ID="YOUR_REDDIT_CLIENT_ID"
    REDDIT_CLIENT_SECRET="YOUR_REDDIT_CLIENT_SECRET"
    REDDIT_USER_AGENT="RedditFlowETL (by /u/YourUsername)"
    MONGO_URI="mongodb://localhost:27017/" 
    ```

3.  **Configure ETL Parameters:**
    Edit the file **`config/config.py`** to set the subreddits you want to target, the sorting method, and the maximum number of posts to fetch per run.

---

## 3. How to Run

Execute the main script from your terminal:

```bash
python main.py
