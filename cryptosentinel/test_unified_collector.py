"""
Test script for unified data collector.

Runs multiple collection cycles with delays to generate test data.
"""

import time
from datetime import datetime
from producers.unified_collector import UnifiedDataCollector
from loguru import logger

def main():
    """Run test collection cycles."""
    
    print("=" * 60)
    print("Unified Data Collector Test")
    print("=" * 60)
    print()
    
    # Initialize collector with unified table mode
    collector = UnifiedDataCollector(
        data_dir="./data/canonical",
        unified_table=True,  # Save all coins to one table
    )
    
    print(f"Registered collectors: {collector.get_registered_collectors()}")
    print(f"Unified table mode: {collector.unified_table}")
    print(f"Data directory: {collector.data_dir}")
    print()
    
    # Coins to collect
    coins = ["bitcoin", "ethereum"]
    
    # Number of collection cycles (adjust as needed)
    num_cycles = 8
    delay_seconds = 10
    
    # You can also customize these:
    # num_cycles = 5   # Fewer cycles for quick test
    # num_cycles = 10  # More cycles for more data
    # delay_seconds = 5  # Faster collection
    
    print(f"Will collect data {num_cycles} times, {delay_seconds} seconds apart")
    print(f"Coins: {', '.join(coins)}")
    print()
    print("Starting collection cycles...")
    print("-" * 60)
    
    for cycle in range(1, num_cycles + 1):
        print(f"\n[Cycle {cycle}/{num_cycles}] {datetime.now().strftime('%H:%M:%S')}")
        
        try:
            # Collect data
            records = collector.collect_all_at_timestamp(coins)
            
            # Save to CSV
            collector.save_to_csv(records, unified_table=True)
            
            # Print summary
            print(f"  ✅ Collected {len(records)} records")
            for record in records:
                price_str = f"${record.price_usd:,.2f}" if record.price_usd else "N/A"
                fng_str = f"{record.fear_greed_value}" if record.fear_greed_value else "N/A"
                news_str = f"{record.news_count} articles" if record.news_count else "no news"
                
                print(f"     {record.coin_id.upper()}: {price_str} | F&G: {fng_str} | {news_str}")
            
            # Wait before next cycle (except last one)
            if cycle < num_cycles:
                print(f"  ⏳ Waiting {delay_seconds} seconds before next cycle...")
                time.sleep(delay_seconds)
                
        except Exception as e:
            logger.error(f"Error in cycle {cycle}: {e}")
            if cycle < num_cycles:
                time.sleep(delay_seconds)
    
    print()
    print("-" * 60)
    print("Collection complete!")
    print()
    
    # Show summary
    unified_file = collector.data_dir / "unified_market_data.csv"
    if unified_file.exists():
        import pandas as pd
        df = pd.read_csv(unified_file, parse_dates=["timestamp"])
        
        print(f"📊 Unified table created: {unified_file}")
        print(f"   Total rows: {len(df)}")
        print(f"   Unique timestamps: {df['timestamp'].nunique()}")
        print(f"   Coins: {df['coin_id'].unique().tolist()}")
        print()
        print("   First few rows:")
        print(df[['timestamp', 'coin_id', 'price_usd', 'fear_greed_value', 'news_count']].head(10).to_string(index=False))
        print()
        print("   Latest rows:")
        print(df[['timestamp', 'coin_id', 'price_usd', 'fear_greed_value', 'news_count']].tail(10).to_string(index=False))
    else:
        print(f"⚠️  Unified table not found at {unified_file}")
    
    print()
    print("=" * 60)
    print("Test complete! Check the CSV file to view the data.")
    print("=" * 60)


if __name__ == "__main__":
    main()

