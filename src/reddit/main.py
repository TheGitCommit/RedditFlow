import logging
import asyncio
import sys
from datetime import datetime
from src.reddit.etl.extractor import RedditExtractor
from src.reddit.etl.loader import MongoLoader
from src.reddit.config.config import FETCH_COMMENTS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../../reddit_etl.log'),
        logging.StreamHandler()
    ]
)


def print_banner(title):
    """
    Prints a formatted banner with a centered title surrounded by separator lines.

    Args:
        title (str): The title text to be displayed and centered in the banner.
    """
    print("\n" + "=" * 70)
    print(title.center(70))
    print("=" * 70 + "\n")


def print_stats_summary(extractor_stats, loader_stats, post_stats, comment_stats, duration):
    """
    Prints a summary of pipeline statistics to the console.

    This function provides detailed statistics related to the performance
    and operational aspects of a data pipeline. It showcases data related to
    extraction, loading, and database totals, including processed posts,
    comments, errors, and runtime information. It also highlights
    subreddit-level statistics and comment depth distribution when available.

    Args:
        extractor_stats (dict): Dictionary containing statistics about the
            data extraction process, such as posts fetched, posts processed,
            comments fetched, errors encountered, skipped checkpoints, and
            rate limit waits.

        loader_stats (dict): Dictionary containing statistics about the data
            loading process, such as posts inserted, posts updated, comments
            inserted, comments updated, and errors encountered during the
            loading stage.

        post_stats (dict): Dictionary providing aggregated statistics about
            posts in the database, including the total number of posts and
            counts grouped by subreddit.

        comment_stats (dict): Dictionary containing statistics about comments
            available in the database, including total comment counts and
            comment depth distribution, if applicable.

        duration (float): The total duration of the pipeline run in seconds,
            used to compute performance metrics such as throughput.
    """
    print_banner("PIPELINE STATISTICS")

    print("PERFORMANCE")
    print(f"  Duration: {duration:.2f} seconds")
    if duration > 0:
        print(f"  Throughput: {extractor_stats.get('posts_processed', 0) / duration:.2f} posts/sec")

    print("\nEXTRACTION")
    print(f"  Posts fetched: {extractor_stats.get('posts_fetched', 0)}")
    print(f"  Posts processed: {extractor_stats.get('posts_processed', 0)}")
    print(f"  Comments fetched: {extractor_stats.get('comments_fetched', 0)}")
    print(f"  Errors: {extractor_stats.get('errors', 0)}")
    print(f"  Skipped (checkpointed): {extractor_stats.get('skipped_checkpoints', 0)}")
    print(f"  Rate limit waits: {extractor_stats.get('rate_limit_waits', 0)}")

    print("\nLOADING")
    print(f"  Posts inserted: {loader_stats.get('posts_inserted', 0)}")
    print(f"  Posts updated: {loader_stats.get('posts_updated', 0)}")
    print(f"  Comments inserted: {loader_stats.get('comments_inserted', 0)}")
    print(f"  Comments updated: {loader_stats.get('comments_updated', 0)}")
    print(f"  Errors: {loader_stats.get('errors', 0)}")

    print("\nDATABASE TOTALS")
    print(f"  Total posts in DB: {post_stats.get('total_posts', 0)}")
    print(f"  Total comments in DB: {comment_stats.get('total_comments', 0)}")

    if post_stats.get('by_subreddit'):
        print("\nPosts by Subreddit:")
        for item in post_stats['by_subreddit'][:5]:
            print(f"    r/{item['_id']}: {item['count']}")

    if FETCH_COMMENTS and comment_stats.get('depth_distribution'):
        print("\nComment Depth Distribution:")
        for item in comment_stats['depth_distribution'][:10]:
            depth_label = "Top-level" if item['_id'] == 0 else f"Level {item['_id']}"
            print(f"    {depth_label}: {item['count']} comments")


async def run_etl_pipeline():
    """
    Executes an ETL (Extract, Transform, Load) pipeline to fetch and process data
    from Reddit, and load it into a MongoDB database.

    This function initializes the required ETL components, handles data extraction
    and loading operations, measures operation statistics, and provides a summary
    of the executed workflow.

    Returns:
        int: 0 if the pipeline completes successfully, 1 if the pipeline fails.

    Raises:
        Exception: If an unexpected error occurs during the pipeline execution.
    """
    extractor = None
    loader = None
    start_time = datetime.now()

    try:
        print_banner("REDDIT ETL PIPELINE STARTING")
        print(f"Comment Fetching: {'ENABLED' if FETCH_COMMENTS else 'DISABLED'}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Initialize components
        extractor = RedditExtractor(use_checkpoints=True, use_rate_limiting=True)
        loader = MongoLoader(batch_size=100)

        # Extract data
        logging.info("Starting data extraction...")
        posts_data, comments_data = await extractor.extract_posts_with_comments()

        # Load posts
        if posts_data:
            logging.info(f"Loading {len(posts_data)} posts into MongoDB...")
            await loader.load_posts(posts_data)
        else:
            logging.warning("No posts to load")

        # Load comments
        if FETCH_COMMENTS and comments_data:
            logging.info(f"Loading {len(comments_data)} comments into MongoDB...")
            await loader.load_comments(comments_data)
        elif FETCH_COMMENTS:
            logging.info("No comments were extracted")

        # Get statistics
        duration = (datetime.now() - start_time).total_seconds()
        extractor_stats = extractor.get_stats()
        loader_stats = loader.get_stats()
        post_stats = loader.get_post_stats()
        comment_stats = loader.get_comment_stats() if FETCH_COMMENTS else {}

        # Display summary
        print_stats_summary(extractor_stats, loader_stats, post_stats, comment_stats, duration)

        print_banner("PIPELINE COMPLETED SUCCESSFULLY")

    except KeyboardInterrupt:
        print_banner("PIPELINE INTERRUPTED BY USER")
        logging.warning("Pipeline interrupted by user")

    except Exception as e:
        print_banner("PIPELINE FAILED")
        logging.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1

    finally:
        if extractor:
            await extractor.close()
        if loader:
            loader.close_connection()

    return 0


async def run_test():
    """
    Executes a test run for the Reddit ETL process.

    This function initializes a RedditExtractor instance and performs a test fetch
    with it. It also manages interruptions and errors, providing appropriate
    handling such as logging errors and releasing resources.

    Raises:
        KeyboardInterrupt: Raised if the test is interrupted by the user.
        Exception: Raised if any other error occurs during the execution.

    Args:
        None

    Returns:
        int: Returns 0 if the test run completes successfully, or 1 if an
        exception occurs during the test.
    """
    extractor = None

    try:
        print_banner("REDDIT ETL TEST MODE")

        extractor = RedditExtractor(use_checkpoints=False, use_rate_limiting=True)
        await extractor.fetch_and_print_test()

    except KeyboardInterrupt:
        print("\nTest interrupted by user")

    except Exception as e:
        logging.error(f"Test failed: {e}", exc_info=True)
        return 1

    finally:
        if extractor:
            await extractor.close()

    return 0


async def clear_checkpoint():
    """
    Clears the checkpoint state for the data extraction process.

    This function uses the CheckpointManager class from the etl.extractor module
    to reset the checkpoint state, ensuring that the next run will process all
    posts without skipping any previously processed data.

    Raises:
        Exception: An exception may be raised during checkpoint clearance if
            the CheckpointManager encounters an issue while clearing the
            checkpoint.
    """
    from src.reddit.etl.extractor import CheckpointManager

    checkpoint = CheckpointManager()
    checkpoint.clear()
    print("Checkpoint cleared! Next run will process all posts.")


def print_help():
    """
    Displays usage information for the Reddit ETL Pipeline. This function provides a detailed overview
    of the script's functionality, configuration options, and supported commands. It outputs an
    instructional guide to help users understand how to operate the pipeline efficiently.

    Displays information about:
    - Running the ETL pipeline.
    - Test mode execution.
    - Resetting with the clear command.
    - Configuring the pipeline behavior (via config/config.py).

    Raises:
        None
    """
    print("""
Reddit ETL Pipeline Usage:

    python main.py              Run full ETL pipeline (extract + load)
    python main.py test         Run in test mode (no database writes)
    python main.py clear        Clear checkpoint (start fresh)
    python main.py help         Show this help message

Configuration:
    Edit config/config.py to change:
    - Subreddits to scrape
    - Number of posts to fetch
    - Enable/disable comment fetching
    - Rate limiting settings
    - MongoDB connection

Examples:
    # Run pipeline with checkpoints (resumes from last run)
    python main.py

    # Test without saving to database
    python main.py test

    # Clear checkpoint and start fresh
    python main.py clear && python main.py
    """)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "test":
            exit_code = asyncio.run(run_test())
            sys.exit(exit_code)

        elif command == "clear":
            asyncio.run(clear_checkpoint())
            sys.exit(0)

        elif command in ["help", "-h", "--help"]:
            print_help()
            sys.exit(0)

        else:
            print(f"Unknown command: {command}")
            print_help()
            sys.exit(1)
    else:
        exit_code = asyncio.run(run_etl_pipeline())
        sys.exit(exit_code)