# Agent Debugging Guide

## Understanding Test Output

### Test Results Format

When you run `test_agent_unified_schema.py`, you'll see output like this:

```
======================================================================
Test 1: DataFrame Loading from Unified CSV
======================================================================
✅ SUCCESS: Loaded 8 rows from unified CSV
   Columns: ['timestamp', 'coin_id', 'price_usd', ...]
   Date range: 2025-12-28 17:07:13 to 2025-12-28 17:17:06
✅ All required columns present
```

**What this means:**
- ✅ = Test passed
- ❌ = Test failed
- ⚠️ = Warning (test may have issues but didn't fail)

### Hypothesis Generation Output

When the agent runs, you'll see logs like:

```
2025-12-28 12:12:05.731 | INFO | agent.orchestrator:_test_confounders_node:528 - Testing confounding factors
2025-12-28 12:12:05.745 | DEBUG | agent.orchestrator:discover:689 - State update: {'test_confounders': {'status': 'CONFOUNDER_TESTING'}}
```

**What this means:**
- Each line shows: `timestamp | log_level | module:function:line - message`
- The agent goes through these stages:
  1. `generate_hypotheses` - Creates causal hypotheses
  2. `discover_data_sources` - Finds where data comes from
  3. `integrate_pipelines` - Sets up Kafka topics (skipped in offline)
  4. `collect_data` - Gathers data (skipped in offline)
  5. `run_causal_tests` - Runs statistical tests
  6. `discover_confounders` - Finds potential confounders
  7. `test_confounders` - Tests for confounding effects
  8. `refine_hypotheses` - Refines hypotheses based on results
  9. `evaluate_results` - Decides if discovery is complete

## The Recursion Limit Error

### What Happened

The error you saw:
```
❌ FAILED: Recursion limit of 25 reached without hitting a stop condition.
```

**Root Cause:**
The agent was stuck in an infinite loop between these nodes:
1. `refine_hypotheses` → `evaluate_results` → `generate_hypotheses` → (repeat)

**Why it happened:**
- In offline mode, `_refine_hypotheses_node` tried to call `hypothesis_generator.refine()`
- But `hypothesis_generator` is `None` in offline mode
- The error was caught, but **iteration counter wasn't incremented**
- So `_should_refine()` kept returning "refine" because `iteration < 3`
- This created an infinite loop: refine → evaluate → refine → evaluate → ...

### The Fix

1. **Offline Mode Handling**: In offline mode, `_refine_hypotheses_node` now:
   - Skips refinement (no Gemini call needed)
   - **Always increments iteration counter** (critical!)
   - Returns existing hypotheses unchanged

2. **Error Handling**: Even if refinement fails, iteration is now incremented to prevent loops

3. **Recursion Limit**: Increased default from 25 to 30, and made it configurable

4. **Safety Check**: Added early break if iteration >= 3 to prevent infinite loops

### How to Verify the Fix

Run the test again:
```bash
cd cryptosentinel
python test_agent_unified_schema.py
```

You should now see:
- ✅ All 5 tests pass
- No recursion limit errors
- Agent completes discovery successfully

## Understanding the Generated CSV

When you see `bitcoin_hyp_test_hyp_1.csv`, this is a hypothesis-specific DataFrame:

```csv
timestamp,coin_id,cause,effect
2025-12-28 17:07:13.779391+00:00,bitcoin,-0.282,87769.0
```

**What this means:**
- `cause` = The cause variable value (e.g., `news_sentiment_avg` = -0.282)
- `effect` = The effect variable value (e.g., `price_usd` = 87769.0)
- This is the data extracted for testing a specific hypothesis

**Column Mapping:**
- `news_sentiment_avg` → `cause` (renamed for analysis)
- `price_usd` → `effect` (renamed for analysis)

## Common Issues and Solutions

### Issue: "No offline data loaded"

**Solution:** Run the data collector first:
```bash
python test_unified_collector.py
```

### Issue: "Variable not found in DataFrame"

**Solution:** Check that:
1. `unified_market_data.csv` exists in `data/canonical/`
2. The CSV has the required columns (see schema in DEVELOPMENT_PROCESS.md)
3. The variable is registered in `asset_registry.py`

### Issue: "Recursion limit reached"

**Solution:** This should be fixed now, but if it happens:
1. Check that iteration counter is incrementing (look for `"iteration": 1, 2, 3` in logs)
2. Verify `_refine_hypotheses_node` handles offline mode correctly
3. Increase recursion limit in config: `{"recursion_limit": 50}`

### Issue: "No significant results found"

**This is normal!** It means:
- The agent ran successfully
- But no statistically significant causal relationships were found
- This could mean:
  - Not enough data (need more rows)
  - No actual causal relationship exists
  - Need different variables or methods

## Debugging Tips

1. **Check the logs**: Look at `agent_discovery.log` or `test_agent_unified_schema.log`

2. **Add debug prints**: In `orchestrator.py`, add:
   ```python
   logger.debug(f"Current iteration: {state['iteration']}")
   logger.debug(f"Should refine: {self._should_refine(state)}")
   ```

3. **Test individual nodes**: You can test nodes separately:
   ```python
   state = {"iteration": 0, "status": "INITIALIZING", ...}
   result = agent._generate_hypotheses_node(state)
   ```

4. **Use sequential mode**: If LangGraph has issues, it falls back to sequential execution automatically

## Next Steps

After fixing the recursion issue:
1. ✅ Run `test_agent_unified_schema.py` - should pass all tests
2. ✅ Run `example_agent_usage.py --offline` - should complete successfully
3. ✅ Check that hypotheses are generated correctly
4. ✅ Verify causal tests run (even if no significant results)

