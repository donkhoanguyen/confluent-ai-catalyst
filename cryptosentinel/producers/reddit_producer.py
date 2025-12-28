"""
Reddit Producer

Streams new posts from crypto subreddits to Kafka.
"""

import json
import time
from datetime import datetime
from typing import Optional

import praw
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from loguru import logger

from config.settings import Settings, get_settings


class RedditProducer:
    """Producer that streams Reddit posts to Kafka."""

    TOPIC = "social.reddit.raw"

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._setup_reddit_client()
        self._setup_kafka_producer()

    def _setup_reddit_client(self):
        """Initialize Reddit API client."""
        self.reddit = praw.Reddit(
            client_id=self.settings.reddit_client_id,
            client_secret=self.settings.reddit_client_secret,
            user_agent=self.settings.reddit_user_agent,
        )
        logger.info("Reddit client initialized")

    def _setup_kafka_producer(self):
        """Initialize Kafka producer with Avro serialization."""
        # Kafka producer
        self.producer = Producer(self.settings.get_kafka_producer_config())

        # Schema Registry client
        schema_registry = SchemaRegistryClient(
            self.settings.get_schema_registry_config()
        )

        # Load Avro schema
        with open("schemas/reddit_post.avsc", "r") as f:
            schema_str = f.read()

        # Create Avro serializer
        self.serializer = AvroSerializer(
            schema_registry,
            schema_str,
            to_dict=lambda post, ctx: post,
        )

        logger.info("Kafka producer initialized")

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Delivery failed for {msg.key()}: {err}")
        else:
            logger.debug(
                f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
            )

    def _post_to_dict(self, submission) -> dict:
        """Convert Reddit submission to dictionary."""
        return {
            "post_id": submission.id,
            "subreddit": submission.subreddit.display_name,
            "title": submission.title,
            "body": submission.selftext[:2000] if submission.selftext else None,
            "score": submission.score,
            "num_comments": submission.num_comments,
            "author": str(submission.author) if submission.author else "[deleted]",
            "url": f"https://reddit.com{submission.permalink}",
            "created_utc": int(submission.created_utc * 1000),
        }

    def stream_posts(self, skip_existing: bool = True):
        """
        Stream new posts from configured subreddits.

        Args:
            skip_existing: If True, only process new posts after starting
        """
        subreddit_str = "+".join(self.settings.tracked_subreddits)
        logger.info(f"Starting Reddit stream for: {subreddit_str}")

        subreddit = self.reddit.subreddit(subreddit_str)

        try:
            for submission in subreddit.stream.submissions(skip_existing=skip_existing):
                try:
                    post = self._post_to_dict(submission)

                    # Serialize with Avro
                    value = self.serializer(
                        post,
                        SerializationContext(self.TOPIC, MessageField.VALUE),
                    )

                    # Produce to Kafka
                    self.producer.produce(
                        topic=self.TOPIC,
                        key=post["post_id"].encode("utf-8"),
                        value=value,
                        callback=self._delivery_callback,
                    )

                    # Trigger delivery callbacks
                    self.producer.poll(0)

                    logger.info(
                        f"📝 r/{post['subreddit']} | {post['title'][:60]}..."
                    )

                except Exception as e:
                    logger.error(f"Error processing submission: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        finally:
            self.producer.flush()
            logger.info("Producer flushed and stopped")

    def fetch_recent_posts(self, limit: int = 100):
        """
        Fetch recent posts (not streaming) for initial data load.

        Args:
            limit: Number of posts to fetch per subreddit
        """
        logger.info(f"Fetching {limit} recent posts per subreddit...")

        for subreddit_name in self.settings.tracked_subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)

                for submission in subreddit.new(limit=limit):
                    post = self._post_to_dict(submission)

                    value = self.serializer(
                        post,
                        SerializationContext(self.TOPIC, MessageField.VALUE),
                    )

                    self.producer.produce(
                        topic=self.TOPIC,
                        key=post["post_id"].encode("utf-8"),
                        value=value,
                        callback=self._delivery_callback,
                    )

                self.producer.poll(0)
                logger.info(f"Fetched {limit} posts from r/{subreddit_name}")

            except Exception as e:
                logger.error(f"Error fetching from r/{subreddit_name}: {e}")

        self.producer.flush()
        logger.info("Initial fetch complete")

    def run(self, mode: str = "stream"):
        """
        Run the producer.

        Args:
            mode: "stream" for live streaming, "fetch" for one-time fetch
        """
        logger.info(f"Starting Reddit producer in {mode} mode")

        while True:
            try:
                if mode == "stream":
                    self.stream_posts()
                elif mode == "fetch":
                    self.fetch_recent_posts()
                    break
                else:
                    raise ValueError(f"Unknown mode: {mode}")

            except Exception as e:
                logger.error(f"Producer error: {e}")
                logger.info("Reconnecting in 30 seconds...")
                time.sleep(30)


def main():
    """Entry point for Reddit producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Reddit Post Producer")
    parser.add_argument(
        "--mode",
        choices=["stream", "fetch"],
        default="stream",
        help="Producer mode: stream (live) or fetch (one-time)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of posts to fetch per subreddit (fetch mode only)",
    )

    args = parser.parse_args()

    producer = RedditProducer()

    if args.mode == "fetch":
        producer.fetch_recent_posts(limit=args.limit)
    else:
        producer.run(mode=args.mode)


if __name__ == "__main__":
    main()

