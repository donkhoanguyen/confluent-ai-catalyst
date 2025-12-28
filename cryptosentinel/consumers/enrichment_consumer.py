"""
Enrichment Consumer

Consumes raw Reddit posts, enriches with Gemini sentiment analysis,
and produces to enriched topic + API.
"""

import json
import time
from datetime import datetime, timezone
from typing import Optional

import requests
import google.genai as genai
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import Settings, get_settings

# Kafka imports - optional
try:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("Kafka libraries not available - running in API-only mode")

# Reddit imports - optional for direct mode
try:
    import praw
    from praw.exceptions import RedditAPIException
    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False
    RedditAPIException = None


SENTIMENT_PROMPT = """Analyze this cryptocurrency-related social media post and extract sentiment information.

Post Title: {title}
Post Body: {body}
Subreddit: r/{subreddit}

Respond with ONLY valid JSON (no markdown, no code blocks):
{{
  "sentiment_label": "POSITIVE" or "NEGATIVE" or "NEUTRAL",
  "sentiment_score": <float from -1.0 (very bearish) to 1.0 (very bullish)>,
  "confidence": <float from 0.0 to 1.0>,
  "mentioned_coins": ["BTC", "ETH", ...],
  "topics": ["price", "adoption", "regulation", "technology", "trading", ...]
}}

Rules:
- For mentioned_coins, use standard symbols (BTC, ETH, SOL, DOGE, ADA, XRP, etc.)
- Only include coins explicitly mentioned or clearly implied
- sentiment_score should reflect bullish/bearish sentiment about crypto markets
- Be conservative with confidence if the post is ambiguous
"""


class EnrichmentConsumer:
    """Consumer that enriches Reddit posts with Gemini sentiment analysis."""

    INPUT_TOPIC = "social.reddit.raw"
    OUTPUT_TOPIC = "social.reddit.enriched"
    CONSUMER_GROUP = "enrichment-consumer-group"
    API_URL = "http://localhost:8000"

    def __init__(self, settings: Optional[Settings] = None, api_url: Optional[str] = None):
        self.settings = settings or get_settings()
        self.api_url = api_url or self.API_URL
        self.kafka_enabled = False
        self.consumer = None
        self.producer = None
        self.deserializer = None
        self.serializer = None
        self.reddit = None
        
        self._setup_gemini()
        self._setup_kafka()
        self._setup_reddit()

    def _setup_gemini(self):
        """Initialize Gemini API client."""
        self.client = genai.Client(api_key=self.settings.gemini_api_key)
        logger.info("Gemini client initialized")

    def _setup_reddit(self):
        """Initialize Reddit client for direct mode."""
        if not PRAW_AVAILABLE:
            logger.info("PRAW not available - direct mode disabled")
            return
        
        # Validate credentials are not placeholders
        client_id = self.settings.reddit_client_id
        client_secret = self.settings.reddit_client_secret
        
        if not client_id or client_id.lower() in ["your-reddit-client-id", ""]:
            logger.error("Reddit CLIENT_ID not configured. Please set REDDIT_CLIENT_ID in .env")
            return
        if not client_secret or client_secret.lower() in ["your-reddit-client-secret", ""]:
            logger.error("Reddit CLIENT_SECRET not configured. Please set REDDIT_CLIENT_SECRET in .env")
            return
            
        try:
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=self.settings.reddit_user_agent,
            )
            # Test authentication by making a simple API call
            try:
                _ = self.reddit.user.me()  # This will fail if auth is invalid, but that's ok for read-only
            except Exception:
                # For read-only access, we don't need user.me() to work
                pass
            logger.info("Reddit client initialized for direct mode")
        except Exception as e:
            logger.error(f"Reddit setup failed: {e}")
            logger.error("Please verify your Reddit API credentials at https://www.reddit.com/prefs/apps")

    def _setup_kafka(self):
        """Initialize Kafka consumer and producer."""
        if not KAFKA_AVAILABLE:
            logger.info("Running without Kafka - direct mode only")
            return
            
        try:
            # Schema Registry
            schema_registry = SchemaRegistryClient(
                self.settings.get_schema_registry_config()
            )

            # Load schemas
            with open("schemas/reddit_post.avsc", "r") as f:
                input_schema = f.read()
            with open("schemas/reddit_post_enriched.avsc", "r") as f:
                output_schema = f.read()

            # Deserializer for input
            self.deserializer = AvroDeserializer(
                schema_registry,
                input_schema,
            )

            # Serializer for output
            self.serializer = AvroSerializer(
                schema_registry,
                output_schema,
                to_dict=lambda post, ctx: post,
            )

            # Consumer
            self.consumer = Consumer(
                self.settings.get_kafka_consumer_config(self.CONSUMER_GROUP)
            )
            self.consumer.subscribe([self.INPUT_TOPIC])

            # Producer for enriched messages
            self.producer = Producer(self.settings.get_kafka_producer_config())

            self.kafka_enabled = True
            logger.info("Kafka consumer and producer initialized")
        except Exception as e:
            logger.warning(f"Kafka setup failed: {e} - running in direct mode only")
            self.kafka_enabled = False

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Enriched message delivered to {msg.topic()}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _extract_sentiment(self, post: dict) -> dict:
        """
        Use Gemini to extract sentiment from a post.

        Args:
            post: Reddit post dictionary

        Returns:
            Sentiment data dictionary
        """
        prompt = SENTIMENT_PROMPT.format(
            title=post.get("title", ""),
            body=post.get("body", "")[:500] if post.get("body") else "",
            subreddit=post.get("subreddit", "unknown"),
        )

        try:
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            text = response.text.strip()

            # Clean up response if wrapped in code blocks
            if text.startswith("```"):
                lines = text.split("\n")
                # Remove first and last lines (code block markers)
                text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
                if text.startswith("json"):
                    text = text[4:].strip()

            sentiment_data = json.loads(text)

            # Validate and normalize
            return {
                "sentiment_label": sentiment_data.get("sentiment_label", "NEUTRAL"),
                "sentiment_score": float(sentiment_data.get("sentiment_score", 0.0)),
                "confidence": float(sentiment_data.get("confidence", 0.5)),
                "mentioned_coins": sentiment_data.get("mentioned_coins", []),
                "topics": sentiment_data.get("topics", []),
            }

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse Gemini response: {e}")
            return self._default_sentiment()
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            raise

    def _default_sentiment(self) -> dict:
        """Return default sentiment when extraction fails."""
        return {
            "sentiment_label": "NEUTRAL",
            "sentiment_score": 0.0,
            "confidence": 0.0,
            "mentioned_coins": [],
            "topics": [],
        }

    def _enrich_post(self, post: dict) -> dict:
        """
        Enrich a post with sentiment data.

        Args:
            post: Raw Reddit post

        Returns:
            Enriched post with sentiment fields
        """
        try:
            sentiment = self._extract_sentiment(post)
        except Exception as e:
            logger.error(f"Sentiment extraction failed: {e}")
            sentiment = self._default_sentiment()

        return {
            **post,
            "sentiment_score": sentiment["sentiment_score"],
            "sentiment_label": sentiment["sentiment_label"],
            "sentiment_confidence": sentiment["confidence"],
            "mentioned_coins": sentiment["mentioned_coins"],
            "topics": sentiment["topics"],
            "processed_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        }

    def _push_to_api(self, enriched: dict):
        """Push enriched post to API for dashboard display."""
        try:
            api_payload = {
                "post_id": enriched.get("post_id", "unknown"),
                "subreddit": enriched.get("subreddit", "unknown"),
                "title": enriched.get("title", ""),
                "body": enriched.get("body"),
                "score": enriched.get("score", 0),
                "sentiment_label": enriched.get("sentiment_label", "NEUTRAL"),
                "sentiment_score": enriched.get("sentiment_score", 0.0),
                "mentioned_coins": enriched.get("mentioned_coins", []),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            
            response = requests.post(
                f"{self.api_url}/api/ingest/post",
                json=api_payload,
                timeout=5,
            )
            response.raise_for_status()
            logger.debug(f"Pushed post to API")
        except Exception as e:
            logger.warning(f"Failed to push to API: {e}")

    def process_message(self, msg):
        """
        Process a single message from Kafka.

        Args:
            msg: Kafka message
        """
        try:
            # Deserialize input
            post = self.deserializer(
                msg.value(),
                SerializationContext(self.INPUT_TOPIC, MessageField.VALUE),
            )

            if not post:
                return

            # Enrich with sentiment
            enriched = self._enrich_post(post)
            
            # Always push to API
            self._push_to_api(enriched)

            # Push to Kafka if enabled
            if self.kafka_enabled and self.serializer and self.producer:
                value = self.serializer(
                    enriched,
                    SerializationContext(self.OUTPUT_TOPIC, MessageField.VALUE),
                )
                self.producer.produce(
                    topic=self.OUTPUT_TOPIC,
                    key=msg.key(),
                    value=value,
                    callback=self._delivery_callback,
                )
                self.producer.poll(0)

            # Log enrichment
            emoji = "🟢" if enriched["sentiment_label"] == "POSITIVE" else \
                    "🔴" if enriched["sentiment_label"] == "NEGATIVE" else "🟡"
            
            logger.info(
                f"{emoji} {enriched['sentiment_label']} ({enriched['sentiment_score']:+.2f}) | "
                f"r/{enriched['subreddit']} | {enriched['title'][:50]}..."
            )

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def process_reddit_post(self, submission) -> Optional[dict]:
        """
        Process a Reddit submission directly (without Kafka).

        Args:
            submission: PRAW submission object

        Returns:
            Enriched post dict or None
        """
        try:
            post = {
                "post_id": submission.id,
                "subreddit": submission.subreddit.display_name,
                "title": submission.title,
                "body": submission.selftext[:2000] if submission.selftext else None,
                "score": submission.score,
                "num_comments": submission.num_comments,
                "author": str(submission.author) if submission.author else "[deleted]",
                "created_utc": int(submission.created_utc * 1000),
            }

            # Enrich with sentiment
            enriched = self._enrich_post(post)
            
            # Push to API
            self._push_to_api(enriched)

            # Log enrichment
            emoji = "🟢" if enriched["sentiment_label"] == "POSITIVE" else \
                    "🔴" if enriched["sentiment_label"] == "NEGATIVE" else "🟡"
            
            logger.info(
                f"{emoji} {enriched['sentiment_label']} ({enriched['sentiment_score']:+.2f}) | "
                f"r/{enriched['subreddit']} | {enriched['title'][:50]}..."
            )

            return enriched

        except Exception as e:
            logger.error(f"Error processing Reddit post: {e}")
            return None

    def run(self):
        """Run the enrichment consumer loop (Kafka mode)."""
        if not self.kafka_enabled:
            logger.error("Kafka not configured. Use run_direct() instead.")
            return
            
        logger.info("Starting enrichment consumer (Kafka mode)...")

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                self.process_message(msg)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()
            logger.info("Consumer closed")

    def run_direct(self, limit: int = 10, interval_seconds: int = 60):
        """
        Run in direct mode - fetch from Reddit, enrich, push to API.
        No Kafka required.

        Args:
            limit: Number of posts to fetch per subreddit per cycle
            interval_seconds: Time between fetch cycles
        """
        if not self.reddit:
            logger.error("Reddit client not configured. Cannot run direct mode.")
            return
            
        logger.info(
            f"Starting enrichment consumer (direct mode, limit={limit}, interval={interval_seconds}s)"
        )

        seen_posts = set()

        try:
            while True:
                for subreddit_name in self.settings.tracked_subreddits:
                    try:
                        subreddit = self.reddit.subreddit(subreddit_name)
                        
                        for submission in subreddit.new(limit=limit):
                            if submission.id in seen_posts:
                                continue
                            
                            seen_posts.add(submission.id)
                            self.process_reddit_post(submission)
                            
                            # Rate limit Gemini calls
                            time.sleep(1)
                            
                    except Exception as e:
                        logger.error(f"Error fetching from r/{subreddit_name}: {e}")

                # Keep seen_posts from growing too large
                if len(seen_posts) > 1000:
                    seen_posts.clear()

                logger.info(f"Sleeping {interval_seconds}s before next fetch...")
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            logger.info("Consumer stopped")

    def fetch_and_enrich_once(self, limit: int = 5):
        """
        Fetch and enrich posts once (for testing).

        Args:
            limit: Number of posts per subreddit
        """
        if not self.reddit:
            logger.error("Reddit client not configured.")
            return []
            
        results = []
        for subreddit_name in self.settings.tracked_subreddits[:2]:  # Limit to 2 subreddits for testing
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                for submission in subreddit.new(limit=limit):
                    enriched = self.process_reddit_post(submission)
                    if enriched:
                        results.append(enriched)
                    time.sleep(1)  # Rate limit
            except RedditAPIException as e:
                if "401" in str(e) or "UNAUTHORIZED" in str(e):
                    logger.error(f"Reddit API authentication failed for subreddit '{subreddit_name}'")
                    logger.error("Please verify your Reddit API credentials:")
                    logger.error("1. Go to https://www.reddit.com/prefs/apps")
                    logger.error("2. Check that your app credentials are correct")
                    logger.error("3. Ensure REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET are set correctly in .env")
                else:
                    logger.error(f"Reddit API error for subreddit '{subreddit_name}': {e}")
            except Exception as e:
                error_msg = str(e)
                if "401" in error_msg or "unauthorized" in error_msg.lower():
                    logger.error(f"Authentication failed for subreddit '{subreddit_name}': {e}")
                    logger.error("Please check your Reddit API credentials in .env file")
                else:
                    logger.error(f"Error fetching from subreddit '{subreddit_name}': {e}")
        return results


def main():
    """Entry point for enrichment consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Enrichment Consumer")
    parser.add_argument(
        "--mode",
        choices=["kafka", "direct"],
        default="direct",
        help="Mode: kafka (consume from Kafka) or direct (fetch from Reddit)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Posts per subreddit (direct mode)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between fetches (direct mode)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch once and exit (for testing)",
    )

    args = parser.parse_args()

    consumer = EnrichmentConsumer()

    if args.once:
        consumer.fetch_and_enrich_once(limit=args.limit)
    elif args.mode == "kafka":
        consumer.run()
    else:
        consumer.run_direct(limit=args.limit, interval_seconds=args.interval)


if __name__ == "__main__":
    main()

