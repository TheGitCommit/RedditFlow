import asyncpraw
import asyncprawcore
import asyncio
import json
import os
from datetime import datetime, timedelta
from src.reddit.config.config import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT,
    SUBREDDITS_TO_SCRAPE, SORTING_METHOD, POST_LIMIT,
    FETCH_COMMENTS, COMMENT_REPLACE_MORE_LIMIT, MAX_COMMENT_DEPTH,
    DELAY_BETWEEN_SUBREDDITS, DELAY_BETWEEN_POSTS
)
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RateLimiter:
    """
    Implements a rate-limiting mechanism to control the frequency of requests.

    The RateLimiter class is designed to ensure that the number of requests to a
    service or resource does not exceed a specified limit within a one-minute
    timeframe. It maintains a record of request timestamps and calculates the
    necessary delays to conform to the rate limit.

    Attributes:
        rpm (int): Maximum number of requests allowed per minute.
        requests (list[datetime]): List of datetime objects representing the
            timestamps of recent requests within the one-minute window.
        total_requests (int): Total number of requests processed by the rate
            limiter.
    """

    def __init__(self, requests_per_minute=60):
        self.rpm = requests_per_minute
        self.requests = []
        self.total_requests = 0

    async def wait_if_needed(self):
        now = datetime.now()
        # Remove requests older than 1 minute
        self.requests = [r for r in self.requests if now - r < timedelta(minutes=1)]

        if len(self.requests) >= self.rpm:
            sleep_time = 60 - (now - self.requests[0]).total_seconds()
            if sleep_time > 0:
                logging.warning(f"Rate limit approaching. Sleeping for {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)

        self.requests.append(now)
        self.total_requests += 1


class CheckpointManager:
    """
    Manages checkpoints for tracking progress in processing tasks.

    This class is designed to handle the tracking of processed items and maintain
    a record of the most recent checkpoint in a file. It allows items to be
    marked as processed, checks if an item has already been processed,
    and provides mechanisms to clear or update the checkpoint.

    Attributes:
        file (str): Name of the file used to store checkpoint data.
        data (dict): Dictionary storing processed items and the last checkpoint
            timestamp.
    """

    def __init__(self, checkpoint_file='checkpoint.json'):
        self.file = checkpoint_file
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, 'r') as f:
                    return json.load(f)
            except:
                logging.warning("Could not load checkpoint, starting fresh")
        return {'processed_posts': [], 'last_run': None}

    def save(self, post_id):
        self.data['processed_posts'].append(post_id)
        self.data['last_run'] = datetime.now().isoformat()
        with open(self.file, 'w') as f:
            json.dump(self.data, f, indent=2)

    def is_processed(self, post_id):
        return post_id in self.data['processed_posts']

    def clear(self):
        """Clear checkpoint - useful for fresh runs"""
        self.data = {'processed_posts': [], 'last_run': None}
        if os.path.exists(self.file):
            os.remove(self.file)


class RedditExtractor:
    """
    Provides methods and utilities to extract and process data from Reddit using the Async PRAW library.

    This class is designed to fetch submissions and their associated data (e.g., metadata, content),
    extract information from individual submissions, and retrieve submission comments with depth tracking.
    It also includes features such as rate limiting, checkpointing, retry logic for API calls, and
    detailed error reporting.

    Attributes:
        reddit (asyncpraw.Reddit): Instance of the Async PRAW Reddit client used for API calls.
        rate_limiter (RateLimiter or None): Optional rate limiter to manage API call frequency.
        checkpoint (CheckpointManager or None): Optional checkpoint manager to track processed data.
        stats (dict): Dictionary for tracking various statistics such as fetched posts, processed posts,
            errors, and rate limit waits.
    """

    def __init__(self, use_checkpoints=True, use_rate_limiting=True):
        try:
            self.reddit = asyncpraw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT,
                timeout=30  # 30 second timeout
            )

            self.rate_limiter = RateLimiter() if use_rate_limiting else None
            self.checkpoint = CheckpointManager() if use_checkpoints else None
            self.stats = {
                'posts_fetched': 0,
                'posts_processed': 0,
                'comments_fetched': 0,
                'errors': 0,
                'skipped_checkpoints': 0,
                'rate_limit_waits': 0
            }

            logging.info("Reddit extractor initialized")
        except Exception as e:
            logging.error(f"Failed to initialize Async PRAW: {e}")
            raise

    async def _api_call_with_retry(self, func, *args, max_retries=3, **kwargs):
        """Execute API call with retry logic and rate limiting"""
        if self.rate_limiter:
            await self.rate_limiter.wait_if_needed()

        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)

            except asyncprawcore.exceptions.TooManyRequests:
                wait_time = 60 * (attempt + 1)
                logging.warning(f"Rate limited! Waiting {wait_time}s (attempt {attempt + 1})")
                self.stats['rate_limit_waits'] += 1
                await asyncio.sleep(wait_time)

            except asyncprawcore.exceptions.ServerError as e:
                logging.error(f"Reddit server error: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(30)

            except asyncprawcore.exceptions.RequestException as e:
                logging.error(f"Request error: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                if attempt == max_retries - 1:
                    self.stats['errors'] += 1
                    return None

        return None

    async def get_submissions(self):
        """Fetches submissions from configured subreddits asynchronously."""
        all_submissions = []

        for subreddit_name in SUBREDDITS_TO_SCRAPE:
            logging.info(f"Fetching {POST_LIMIT} '{SORTING_METHOD}' posts from r/{subreddit_name}...")

            try:
                subreddit = await self._api_call_with_retry(self.reddit.subreddit, subreddit_name)
                if not subreddit:
                    logging.error(f"Could not fetch subreddit r/{subreddit_name}")
                    continue

                # Select subreddit sorting method
                if SORTING_METHOD == 'new':
                    submissions = subreddit.new(limit=POST_LIMIT)
                elif SORTING_METHOD == 'top':
                    submissions = subreddit.top(limit=POST_LIMIT, time='day')
                elif SORTING_METHOD == 'hot':
                    submissions = subreddit.hot(limit=POST_LIMIT)
                else:
                    logging.warning(f"Unknown sorting method '{SORTING_METHOD}'. Using 'hot'.")
                    submissions = subreddit.hot(limit=POST_LIMIT)

                count = 0
                async for submission in submissions:
                    # Skip if already processed (checkpoint)
                    if self.checkpoint and self.checkpoint.is_processed(submission.id):
                        logging.debug(f"Skipping {submission.id} (already processed)")
                        self.stats['skipped_checkpoints'] += 1
                        continue

                    all_submissions.append(submission)
                    count += 1

                logging.info(f"Fetched {count} posts from r/{subreddit_name}")
                await asyncio.sleep(DELAY_BETWEEN_SUBREDDITS)

            except Exception as e:
                logging.error(f"Error fetching from r/{subreddit_name}: {e}")
                self.stats['errors'] += 1
                continue

        self.stats['posts_fetched'] = len(all_submissions)
        logging.info(f"Total submissions fetched: {len(all_submissions)}")
        return all_submissions

    async def get_submission_data(self, submission):
        """Extracts data from an Async PRAW Submission object with validation."""
        try:
            # Ensure submission is fully loaded
            if not hasattr(submission, 'selftext'):
                await self._api_call_with_retry(submission.load)

            data = {
                # Basic identifiers
                'reddit_id': submission.id,
                'name': submission.name,
                'permalink': submission.permalink,
                'url': submission.url,

                # Post content
                'title': submission.title,
                'selftext': getattr(submission, 'selftext', ''),
                'selftext_html': getattr(submission, 'selftext_html', None),

                # Subreddit info
                'subreddit': submission.subreddit.display_name,
                'subreddit_id': submission.subreddit_id,

                # Author info
                'author': submission.author.name if submission.author else '[deleted]',
                'author_fullname': getattr(submission, 'author_fullname', None),

                # Timestamps
                'created_utc': submission.created_utc,
                'created': datetime.fromtimestamp(submission.created_utc),

                # Engagement metrics
                'score': submission.score,
                'upvote_ratio': submission.upvote_ratio,
                'num_comments': submission.num_comments,
                'num_crossposts': getattr(submission, 'num_crossposts', 0),

                # Post type and flags
                'is_self': submission.is_self,
                'is_video': submission.is_video,
                'is_original_content': getattr(submission, 'is_original_content', False),
                'over_18': submission.over_18,
                'spoiler': submission.spoiler,
                'stickied': submission.stickied,
                'locked': getattr(submission, 'locked', False),
                'archived': getattr(submission, 'archived', False),

                # Media
                'thumbnail': getattr(submission, 'thumbnail', None),
                'link_flair_text': submission.link_flair_text,
                'link_flair_css_class': getattr(submission, 'link_flair_css_class', None),
                'domain': getattr(submission, 'domain', None),

                # Awards
                'gilded': getattr(submission, 'gilded', 0),
                'total_awards_received': getattr(submission, 'total_awards_received', 0),

                # Metadata
                'extracted_at': datetime.now()
            }

            # Basic validation
            if not data['reddit_id'] or not data['title']:
                raise ValueError("Missing required fields")

            return data

        except Exception as e:
            logging.error(f"Error extracting submission {submission.id}: {e}")
            self.stats['errors'] += 1
            return None

    def _calculate_comment_depth(self, comment, comment_map):
        """Calculates the actual nesting depth of a comment."""
        depth = 0
        current_parent = comment.parent_id
        max_iterations = 50  # Prevent infinite loops

        while current_parent and current_parent.startswith('t1_') and depth < max_iterations:
            parent_id = current_parent.replace('t1_', '')
            if parent_id in comment_map:
                current_parent = comment_map[parent_id]
                depth += 1
            else:
                break

        return depth

    async def get_submission_comments(self, submission):
        """Fetches all comments for a submission with proper depth tracking."""
        if not FETCH_COMMENTS:
            return []

        logging.info(f"Fetching comments for submission {submission.id}...")

        try:
            # Ensure submission is fully loaded
            await self._api_call_with_retry(submission.load)

            # Check if there are any comments
            if not submission.comments or submission.num_comments == 0:
                logging.info(f"No comments for submission {submission.id}")
                return []

            # Replace "MoreComments" objects
            await self._api_call_with_retry(
                submission.comments.replace_more,
                limit=COMMENT_REPLACE_MORE_LIMIT
            )

            comment_list = submission.comments.list()

            if not comment_list:
                logging.info(f"No comments available after replace_more for {submission.id}")
                return []

            # Build parent map for depth calculation
            comment_map = {}
            for comment in comment_list:
                try:
                    if hasattr(comment, 'id') and hasattr(comment, 'parent_id'):
                        comment_map[comment.id] = comment.parent_id
                except (AttributeError, TypeError):
                    continue

            all_comments = []

            for comment in comment_list:
                try:
                    # Skip MoreComments objects
                    if not hasattr(comment, 'body'):
                        continue

                    depth = self._calculate_comment_depth(comment, comment_map)

                    # Skip if exceeding max depth
                    if MAX_COMMENT_DEPTH is not None and depth > MAX_COMMENT_DEPTH:
                        continue

                    comment_data = await self.get_comment_data(comment, submission.id, depth)
                    if comment_data:
                        all_comments.append(comment_data)

                except (AttributeError, TypeError) as e:
                    logging.debug(f"Skipping comment: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Error extracting comment: {e}")
                    self.stats['errors'] += 1
                    continue

            self.stats['comments_fetched'] += len(all_comments)
            logging.info(f"Fetched {len(all_comments)} comments for submission {submission.id}")
            return all_comments

        except Exception as e:
            logging.error(f"Failed to fetch comments for {submission.id}: {e}")
            self.stats['errors'] += 1
            return []

    async def get_comment_data(self, comment, submission_id, depth):
        """Extracts data from an Async PRAW Comment object."""
        try:
            data = {
                # Identifiers
                'reddit_id': comment.id,
                'name': getattr(comment, 'name', f"t1_{comment.id}"),
                'permalink': getattr(comment, 'permalink', ''),

                # Link to parent submission
                'submission_id': submission_id,
                'link_id': getattr(comment, 'link_id', ''),

                # Comment hierarchy
                'parent_id': getattr(comment, 'parent_id', ''),
                'is_root': getattr(comment, 'is_root', False),
                'depth': depth,

                # Content
                'body': getattr(comment, 'body', '[deleted]'),
                'body_html': getattr(comment, 'body_html', None),

                # Author info
                'author': comment.author.name if hasattr(comment, 'author') and comment.author else '[deleted]',
                'author_fullname': getattr(comment, 'author_fullname', None),
                'author_flair_text': getattr(comment, 'author_flair_text', None),

                # Timestamps
                'created_utc': getattr(comment, 'created_utc', 0),
                'created': datetime.fromtimestamp(getattr(comment, 'created_utc', 0)),
                'edited': comment.edited if hasattr(comment, 'edited') and comment.edited else False,

                # Engagement metrics
                'score': getattr(comment, 'score', 0),
                'ups': getattr(comment, 'ups', 0),
                'downs': getattr(comment, 'downs', 0),
                'controversiality': getattr(comment, 'controversiality', 0),

                # Flags
                'stickied': getattr(comment, 'stickied', False),
                'distinguished': getattr(comment, 'distinguished', None),
                'is_submitter': getattr(comment, 'is_submitter', False),

                # Subreddit
                'subreddit': comment.subreddit.display_name if hasattr(comment, 'subreddit') else '',
                'subreddit_id': getattr(comment, 'subreddit_id', ''),

                # Awards
                'gilded': getattr(comment, 'gilded', 0),
                'total_awards_received': getattr(comment, 'total_awards_received', 0),

                # Metadata
                'extracted_at': datetime.now()
            }
            return data

        except Exception as e:
            logging.error(f"Error creating comment data: {e}")
            return None

    async def extract_posts_with_comments(self):
        """Main extraction method that fetches both submissions and their comments."""
        submissions = await self.get_submissions()

        posts_data = []
        comments_data = []

        for idx, submission in enumerate(submissions, 1):
            try:
                # Extract submission data
                post_data = await self.get_submission_data(submission)

                if not post_data:
                    logging.warning(f"Skipping submission {submission.id} - failed to extract data")
                    continue

                posts_data.append(post_data)
                self.stats['posts_processed'] += 1
                logging.info(f"[{idx}/{len(submissions)}] Extracted post: {post_data['reddit_id']}")

                # Extract comments if enabled
                if FETCH_COMMENTS:
                    submission_comments = await self.get_submission_comments(submission)
                    comments_data.extend(submission_comments)

                    # Rate limiting between posts
                    if idx < len(submissions):
                        await asyncio.sleep(DELAY_BETWEEN_POSTS)

                # Save checkpoint
                if self.checkpoint:
                    self.checkpoint.save(post_data['reddit_id'])

            except Exception as e:
                logging.error(f"Failed to process submission {submission.id}: {e}")
                self.stats['errors'] += 1
                continue

        logging.info(f"Extraction complete: {len(posts_data)} posts, {len(comments_data)} comments")
        return posts_data, comments_data

    def get_stats(self):
        """
        Retrieves a copy of the current statistics.

        This method returns a copy of the statistics dictionary, ensuring that
        the original data remains unaltered when accessed externally.

        Returns:
            dict: A copy of the current statistics dictionary.
        """
        return self.stats.copy()

    async def close(self):
        """Closes the underlying aiohttp session."""
        await self.reddit.close()
        logging.info("Reddit client closed.")

    async def fetch_and_print_test(self):
        """Fetches submissions with comments and prints test data."""
        submissions = await self.get_submissions()

        print("\n" + "=" * 70)
        print("TEST FETCH RESULTS")
        print("=" * 70)
        print(f"FETCH_COMMENTS: {FETCH_COMMENTS}")
        print(f"Processing first 3 posts...")
        print("=" * 70)

        for idx, submission in enumerate(submissions[:3], 1):
            try:
                post_data = await self.get_submission_data(submission)

                if not post_data:
                    continue

                print(f"\n[POST {idx}]")
                print(f"  ID: {post_data['reddit_id']}")
                print(f"  Title: {post_data['title'][:70]}...")
                print(f"  Subreddit: r/{post_data['subreddit']}")
                print(f"  Score: {post_data['score']} | Comments: {post_data['num_comments']}")
                print(f"  Author: {post_data['author']}")
                print(f"  Created: {post_data['created']}")

                if FETCH_COMMENTS:
                    comments = await self.get_submission_comments(submission)
                    print(f"  Fetched {len(comments)} comments")

                    if comments:
                        # Show depth distribution
                        depth_counts = {}
                        for c in comments:
                            depth = c['depth']
                            depth_counts[depth] = depth_counts.get(depth, 0) + 1

                        print(f"  Depth distribution: {dict(sorted(depth_counts.items()))}")

                        # Show sample comment
                        sample = comments[0]
                        print(f"  Sample comment (depth {sample['depth']}):")
                        print(f"    '{sample['body'][:80]}...'")
                else:
                    print(f"  Comment fetching disabled")

            except Exception as e:
                logging.error(f"Error in test for submission: {e}")
                continue

        print("\n" + "=" * 70)
        print("TEST COMPLETE")
        print(f"Statistics: {self.get_stats()}")
        print("=" * 70)

        return submissions