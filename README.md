SQE - Streaming Query Engine
===================

## What is SQE?

SQE is a query engine written using Storm's Trident framework that takes a set of SQL-like (inspired?) commands, including queries, and runs them against one or more input streams. By using Trident, input streams are processed in micro-batches with both good latency and high throughput while guaranteeing "exactly-once" processing. Results can be returned through a list of output streams or SQE can handle persisting to a data store directly using one of its supported Trident states. SQE is designed to make it easy to query against large streams of data with good performance for many different use cases.

There are two external jars included in the repo, both forks of Storm external projects. The forked code can be found here: https://github.com/jwplayer/storm/tree/1.x-redis-hash

https://github.com/jwplayer/sqe/wiki
