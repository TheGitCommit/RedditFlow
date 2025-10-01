import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError
from src.reddit.config.config import MONGO_URI, MONGO_DATABASE, POSTS_COLLECTION, COMMENTS_COLLECTION

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MongoLoader:
    """Handles MongoDB operations for efficient data insertion, updating, and statistics
    retrieval.

    This class is designed to manage batches of data for posts and comments, allowing
    efficient database operations using MongoDB. Various statistics such as the number
    of inserted/updated documents, and error counts, are collected during the operation.
    It also supports generating statistical reports on posts and comments.

    Attributes:
        batch_size (int): Number of documents to process in each batch during insertion or
            updates.
        stats (dict): Dictionary containing statistics about the number of inserted,
            updated, and erroneous operations for posts and comments.
    """

    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.stats = {
            'posts_inserted': 0,
            'posts_updated': 0,
            'comments_inserted': 0,
            'comments_updated': 0,
            'errors': 0
        }

        try:
            self.client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=10000
            )

            # Test connection
            self.client.server_info()

            self.db = self.client[MONGO_DATABASE]
            self.posts_collection = self.db[POSTS_COLLECTION]
            self.comments_collection = self.db[COMMENTS_COLLECTION]

            # Create indexes
            self._create_indexes()

            logging.info(f"Connected to MongoDB: {MONGO_DATABASE}")
        except ServerSelectionTimeoutError:
            logging.error("Could not connect to MongoDB - is it running?")
            raise
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _create_indexes(self):
        """Creates indexes for efficient querying."""
        try:
            # Posts indexes
            self.posts_collection.create_index("reddit_id", unique=True)
            self.posts_collection.create_index("subreddit")
            self.posts_collection.create_index("created_utc")
            self.posts_collection.create_index([("subreddit", 1), ("created_utc", -1)])

            # Comments indexes
            self.comments_collection.create_index("reddit_id", unique=True)
            self.comments_collection.create_index("submission_id")
            self.comments_collection.create_index("parent_id")
            self.comments_collection.create_index("depth")
            self.comments_collection.create_index([("submission_id", 1), ("depth", 1)])
            self.comments_collection.create_index([("submission_id", 1), ("score", -1)])

            logging.info("MongoDB indexes created/verified")
        except Exception as e:
            logging.warning(f"Index creation warning: {e}")

    async def load_posts(self, posts_data):
        """Loads posts into MongoDB using batch upsert operations."""
        if not posts_data:
            logging.warning("No posts to load")
            return

        try:
            # Process in batches
            for i in range(0, len(posts_data), self.batch_size):
                batch = posts_data[i:i + self.batch_size]

                operations = [
                    UpdateOne(
                        {'reddit_id': post['reddit_id']},
                        {'$set': post},
                        upsert=True
                    )
                    for post in batch
                ]

                try:
                    result = self.posts_collection.bulk_write(operations, ordered=False)

                    self.stats['posts_inserted'] += result.upserted_count
                    self.stats['posts_updated'] += result.modified_count

                    logging.info(
                        f"Batch {i // self.batch_size + 1}: "
                        f"{result.upserted_count} inserted, "
                        f"{result.modified_count} updated"
                    )

                except BulkWriteError as e:
                    # Some operations succeeded, log the failures
                    self.stats['errors'] += len(e.details.get('writeErrors', []))
                    logging.error(f"Bulk write errors: {len(e.details.get('writeErrors', []))}")

            logging.info(
                f"Posts load complete: {self.stats['posts_inserted']} inserted, "
                f"{self.stats['posts_updated']} updated"
            )

        except Exception as e:
            logging.error(f"Failed to load posts: {e}")
            self.stats['errors'] += 1

    async def load_comments(self, comments_data):
        """Loads comments into MongoDB using batch upsert operations."""
        if not comments_data:
            logging.warning("No comments to load")
            return

        try:
            # Process in batches
            for i in range(0, len(comments_data), self.batch_size):
                batch = comments_data[i:i + self.batch_size]

                operations = [
                    UpdateOne(
                        {'reddit_id': comment['reddit_id']},
                        {'$set': comment},
                        upsert=True
                    )
                    for comment in batch
                ]

                try:
                    result = self.comments_collection.bulk_write(operations, ordered=False)

                    self.stats['comments_inserted'] += result.upserted_count
                    self.stats['comments_updated'] += result.modified_count

                    logging.info(
                        f"Batch {i // self.batch_size + 1}: "
                        f"{result.upserted_count} inserted, "
                        f"{result.modified_count} updated"
                    )

                except BulkWriteError as e:
                    self.stats['errors'] += len(e.details.get('writeErrors', []))
                    logging.error(f"Bulk write errors: {len(e.details.get('writeErrors', []))}")

            logging.info(
                f"Comments load complete: {self.stats['comments_inserted']} inserted, "
                f"{self.stats['comments_updated']} updated"
            )

        except Exception as e:
            logging.error(f"Failed to load comments: {e}")
            self.stats['errors'] += 1

    def get_post_stats(self):
        """Returns statistics about stored posts."""
        try:
            total_posts = self.posts_collection.count_documents({})

            subreddit_counts = list(self.posts_collection.aggregate([
                {'$group': {'_id': '$subreddit', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]))

            recent_posts = list(self.posts_collection.aggregate([
                {'$sort': {'created_utc': -1}},
                {'$limit': 5},
                {'$project': {'reddit_id': 1, 'title': 1, 'score': 1, 'created': 1}}
            ]))

            return {
                'total_posts': total_posts,
                'by_subreddit': subreddit_counts,
                'recent_posts': recent_posts
            }
        except Exception as e:
            logging.error(f"Failed to get post stats: {e}")
            return {}

    def get_comment_stats(self):
        """Returns statistics about stored comments."""
        try:
            total_comments = self.comments_collection.count_documents({})

            depth_distribution = list(self.comments_collection.aggregate([
                {'$group': {'_id': '$depth', 'count': {'$sum': 1}}},
                {'$sort': {'_id': 1}}
            ]))

            comments_by_post = list(self.comments_collection.aggregate([
                {'$group': {'_id': '$submission_id', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 5}
            ]))

            top_comments = list(self.comments_collection.aggregate([
                {'$sort': {'score': -1}},
                {'$limit': 5},
                {'$project': {'reddit_id': 1, 'body': 1, 'score': 1, 'depth': 1}}
            ]))

            return {
                'total_comments': total_comments,
                'depth_distribution': depth_distribution,
                'top_commented_posts': comments_by_post,
                'top_comments': top_comments
            }
        except Exception as e:
            logging.error(f"Failed to get comment stats: {e}")
            return {}

    def get_stats(self):
        """Return loader statistics"""
        return self.stats.copy()

    def close_connection(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            logging.info("MongoDB connection closed")