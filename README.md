This repo is migrated from another repo.

Due to lack of GCP pub/sub supporting in big data ecosystem, I created this lib for streaming ingestion from GCP pub/sub service to cloud storage. This lib was used in Databricks env initially, but can be easily modified to adapt to a common python env.
This lib guarantees no data loss. But no implementation of exactly once semantic.
For HA, you can run this lib in multiple instances.
