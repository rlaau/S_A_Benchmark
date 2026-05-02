## Problem and Goal

1. Problem: In situations where extreme data distributions appear, existing benchmarks alone make it difficult to predict whether an algorithm or database will work properly.

- In particular, blockchain data requires specialized benchmarks that account for factors such as automatic distribution effects from uniform ID distribution caused by hashes, massive cache misses caused by the rate of new wallet creation, and extreme skew in activity rates across wallets.
- Example: latency under cache-miss-heavy conditions, throughput when only the top 1% results in cache hits, Bloom filter performance when there are no data updates, and so on.

2. Goal: Find algorithms that can maintain proper performance even in specialized environments.
