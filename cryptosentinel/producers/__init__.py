"""Kafka producers and data collectors for CryptoSentinel data ingestion."""

# Lazy imports to avoid dependency issues
__all__ = [
    "RedditProducer",
    "PriceProducer",
    "FearGreedCollector",
    "NewsDataCollector",
    "UnifiedDataCollector",
]


def __getattr__(name):
    if name == "RedditProducer":
        from .reddit_producer import RedditProducer
        return RedditProducer
    elif name == "PriceProducer":
        from .price_producer import PriceProducer
        return PriceProducer
    elif name == "FearGreedCollector":
        from .fear_greed_collector import FearGreedCollector
        return FearGreedCollector
    elif name == "NewsDataCollector":
        from .news_collector import NewsDataCollector
        return NewsDataCollector
    elif name == "UnifiedDataCollector":
        from .unified_collector import UnifiedDataCollector
        return UnifiedDataCollector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

