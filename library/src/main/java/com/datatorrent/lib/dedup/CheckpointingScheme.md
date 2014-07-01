Introduction
============
De-duplicator operator (deduper) eliminates duplicate events from the processing pipeline. An event is considered a duplicate if it was seen within a configurable time-range.
This range could be large and extend to days. In that case the state of the deduper will grow substantially and can cause regular check-pointing to take a long time to save this state.

Incremental Check-pointing
==========================
The deduper employs a bucketing system where new events are persisted every window. The bucket manager groups events in buckets based on a criterion that can be easily overriden.
For each bucket, the manager maintain two sub-buckets:
  * transient sub-bucket: events which are already persisted. This state is not check-pointed by the engine.
  * non-transient sub-bucket: events which have not been persisted yet.

At the end of every application window the bucket manager writes the non-transient sub-bucket of all the buckets and transfers the data from them to the corresponding transient sub-bucket.
During recovery the buckets are lazily loaded from the persistent store which also reduces the time in reconstructing the operator's state when it is re-deployed.

