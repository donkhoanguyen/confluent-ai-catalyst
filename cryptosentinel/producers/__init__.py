"""Kafka producers for CryptoSentinel data ingestion."""

# Lazy imports to avoid dependency issues
__all__ = ["RedditProducer", "PriceProducer"]

def __getattr__(name):
    if name == "RedditProducer":
        from .reddit_producer import RedditProducer
        return RedditProducer
    elif name == "PriceProducer":
        from .price_producer import PriceProducer
        return PriceProducer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

