
**1. How do synchronous and asynchronous architectures differ when designing high-throughput LLM inference systems?**
Synchronous architectures require the client to block and wait for a response, making them highly vulnerable to resource exhaustion during long-running LLM generation. Asynchronous architectures decouple the request from the response using queues or event buses, allowing the server to acknowledge receipt immediately and process the heavy compute task in the background, freeing up web threads.

**2. What are the core components and advantages of an event-driven system in complex AI workflows?**
Event-driven systems rely on events (state changes) to trigger asynchronous communication between decoupled microservices. Core components include event producers, event routers/brokers, and event consumers. For AI workflows, this allows independent scaling of components (e.g., audio ingestion vs. heavy transcription processing) and easy introduction of new downstream consumers without modifying the producer.

**3. How does the Producer/Consumer pattern optimize batch processing for tasks like embedding generation?**
The Producer/Consumer pattern decouples the generation of work from its execution using a shared buffer (queue). A fast producer (e.g., a service scraping web documents) can push items into the queue at high speed, while a slower consumer pool (GPU-bound embedding generators) pulls items at its own pace. This prevents the producer from overwhelming the consumer and absorbs traffic spikes.

**4. What is backpressure, and how should a resilient event-driven AI system handle it?**
Backpressure occurs when a data consumer cannot process events as fast as the producer generates them. Resilient systems handle backpressure by buffering (queuing), dropping messages (load shedding), or signaling the producer to slow down (rate limiting).

**5. Why is ordering notoriously difficult to guarantee in distributed message queues, and how can it be achieved?**
In distributed queues, messages are often processed by multiple competing consumers concurrently, meaning a later message might finish processing before an earlier one. Strict ordering can only be achieved by routing related messages to the same partition (e.g., using a Kafka partition key based on `session_id`) and processing that partition with a single consumer thread.

**6. What are the key differences between a Message Queue (e.g., SQS) and a Pub/Sub system (e.g., SNS)?**
A Message Queue typically uses point-to-point communication where a message is consumed by exactly one worker (competing consumers). A Pub/Sub system broadcasts a single event to multiple independent subscribers simultaneously, allowing distinct microservices to react to the same event in different ways.

**7. How do competing consumers improve failure recovery and throughput?**
By attaching multiple instances of a consumer service to a single queue, the workload is distributed. If one consumer crashes while processing a document, the message becomes visible again and is automatically picked up by another healthy consumer, ensuring continuous processing and inherent high availability.

## Delivery Semantics & Reliability

**8. Explain At-most-once, At-least-once, and Exactly-once delivery semantics.**

* **At-most-once:** The message is delivered once or lost (fire-and-forget). Low latency, but risks data loss.
* **At-least-once:** The message is guaranteed to arrive, but might be delivered multiple times if acknowledgments fail.
* **Exactly-once:** The ideal scenario where a message is processed exactly one time without loss or duplication.

**9. Why is true Exactly-once delivery considered theoretically impossible in distributed systems over an unreliable network?**
Due to the Two Generals' Problem, a sender cannot know for certain if a network partition destroyed the message itself or just the acknowledgment of a successful process. To guarantee the message arrives (At-least-once), the sender must retry upon timeout, which inherently introduces the possibility of duplicates if the original message actually succeeded.

**10. How do modern distributed systems achieve *effective* Exactly-once processing?**
Systems achieve effective Exactly-once processing by combining **At-least-once delivery** with **Idempotent consumers**. Even if the message broker delivers the message multiple times, the consumer's logic ensures that processing the duplicate has no additional effect on the system's state.

**11. What is idempotency, and how do you design an idempotent endpoint for an AI agent action?**
Idempotency means an operation produces the same result regardless of how many times it is executed. For AI APIs, this is achieved by requiring clients to send a unique `idempotency_key` with their request. The server checks a fast cache/database for this key; if a result already exists, it returns the cached response instead of re-triggering the expensive agent workflow.

**12. What role does a Visibility Timeout play in message queuing systems?**
When a consumer pulls a message from a queue, the visibility timeout temporarily hides that message from other consumers. If the consumer processes the message and deletes it before the timeout expires, the message is gone. If the consumer crashes and fails to delete it, the timeout expires, and the message reappears for another worker to process.

**13. Why must visibility timeouts be dynamically managed for long-running LLM inference tasks?**
LLM generation times vary wildly based on prompt complexity and output length. If a static visibility timeout is too short, a slow but successful generation will exceed the timeout, causing the message to reappear in the queue and trigger duplicate parallel processing. Consumers must emit "heartbeats" to extend the visibility timeout dynamically while they are actively working.

**14. What are Dead-Letter Queues (DLQs), and what is their role in failure recovery?**
A DLQ is a secondary queue where the main queue routes messages that have failed processing multiple times (exceeded the max receive count). This isolates "poison pill" messages (e.g., malformed payloads causing consumer crashes) from healthy traffic, allowing engineers to inspect, debug, and manually redrive the failed messages without blocking the main pipeline.

**15. What are the best practices for handling duplicate messages in a stateless consumer?**
Stateless consumers must rely on an external state store to detect duplicates. Best practices include using database unique constraints (e.g., upserting based on a deterministic transaction ID), utilizing distributed caches (Redis) to store recently processed message IDs, or employing Bloom filters for space-efficient probabilistic duplicate detection.

## Queue Mechanics & Scaling

**16. Why is simple linear retrying dangerous, and how does Exponential Backoff solve it?**
Linear retries (e.g., retrying every 1 second) can overwhelm a struggling downstream service, causing a cascading failure or "thundering herd." Exponential backoff increases the wait time exponentially between consecutive failures (e.g., 1s, 2s, 4s, 8s), giving the failing service time to recover.

**17. What is "Jitter," and why must it be combined with exponential backoff?**
If multiple consumers encounter a failure simultaneously, exponential backoff alone will cause them to retry at the exact same exponential intervals, repeatedly creating synchronized spikes in traffic. Jitter adds randomness to the backoff duration, spreading the retries out over time and smoothing the load.

**18. How do you scale consumer workers dynamically based on Queue Depth?**
Queue depth (the number of pending messages) is a primary metric for autoscaling. However, instead of raw queue depth, scaling policies should evaluate the **backlog per worker** (queue depth divided by active consumers) or the **average age of the oldest message**. This ensures workers scale up when processing lags, rather than just reacting to brief, manageable traffic spikes.

**19. What is a "poison pill" message, and how does it impact queue depth and scaling?**
A poison pill is a message that consistently causes the consumer application to crash or throw an unhandled exception before it can be acknowledged. Without a DLQ or max-retry limit, the message returns to the queue indefinitely, repeatedly crashing workers, causing false queue depth spikes, and triggering unnecessary infrastructure scaling.

**20. In high-throughput systems, what are the trade-offs between "pull-based" and "push-based" message consumption?**

* **Pull-based (e.g., Kafka, SQS):** Consumers poll for messages at their own pace. Excellent for handling backpressure naturally, but introduces slight latency due to polling intervals.
* **Push-based (e.g., Webhooks, RabbitMQ):** The broker immediately pushes messages to consumers. Offers lower latency but requires the consumer to implement strict backpressure mechanisms to avoid being overwhelmed.

**21. How do you gracefully handle failure recovery when a consumer node is abruptly terminated (e.g., AWS Spot Instance interruption)?**
The system must rely on the queue's visibility timeout. Because the abruptly terminated worker cannot explicitly acknowledge or reject the message, the broker simply waits for the timeout to expire. To minimize delays, workers should listen for OS termination signals (SIGTERM) and stop pulling new messages while finishing their current in-flight task.

**22. How does partitioning in a message broker (like Kafka) affect consumer scaling limits?**
In partitioned brokers, ordering is only guaranteed within a partition, and a single partition can only be consumed by one worker in a consumer group at a time. Therefore, the maximum number of concurrent workers you can scale up to is strictly limited to the number of partitions configured for that topic.

## Consistency & Distributed Transactions

**23. Define Eventual Consistency and why it is prevalent in highly available distributed systems.**
Eventual consistency guarantees that if no new updates are made to a given data item, all accesses to that item will eventually return the last updated value. It is prevalent because the CAP theorem states that in the presence of network partitions, systems must choose between high availability and strict consistency. Favoring availability means accepting temporary data staleness.

**24. What are distributed transactions, and why are they generally avoided in modern microservice architectures?**
Distributed transactions attempt to ensure ACID properties across multiple independent databases or services. They are typically avoided because traditional protocols like Two-Phase Commit (2PC) require locking resources across network boundaries, causing severe performance bottlenecks, high latency, and massive points of failure if the coordinator crashes.

**25. How does the Saga Pattern provide an alternative to distributed transactions?**
A Saga breaks a distributed transaction into a sequence of local database transactions. Each local transaction updates the local state and publishes an event to trigger the next step. If a step fails, the Saga executes **compensating transactions** to undo the changes made by the preceding successful steps, eventually restoring the system to a consistent state.

**26. What is a Compensating Transaction, and how does it differ from a traditional database rollback?**
A traditional rollback occurs before a transaction is committed, leaving no trace. A compensating transaction is a completely new business operation designed to logically reverse a *previously committed* transaction (e.g., issuing a refund for a completed payment).

**27. What is the Transactional Outbox Pattern, and what critical problem does it solve?**
When a microservice needs to update its database and publish an event to a queue, dual-writing can fail partially (e.g., DB commits, but the queue is down). The Outbox Pattern solves this by saving the event to an "outbox" table in the *same* database transaction as the state change. A separate background process then reliably polls the outbox table and publishes the events to the queue At-least-once.

**28. Compare Choreography vs. Orchestration when implementing the Saga pattern.**

* **Choreography:** Services react to events published by other services without centralized control. It is highly decoupled but can become difficult to track and debug in complex workflows.
* **Orchestration:** A central controller (orchestrator) explicitly tells participant services what local transactions to execute. It creates a single point of failure/logic but drastically simplifies complex state machines and error handling.

**29. What is "Read-After-Write" consistency, and how do you manage its absence in eventually consistent user interfaces?**
Read-after-write consistency ensures that once a user submits an update, their subsequent reads will immediately reflect that update. In eventually consistent systems where this is absent, UIs manage the gap via optimistic UI updates (updating the client locally while waiting for backend propagation) or by forcefully routing the user's immediate subsequent reads to the primary database node.

**30. How do the concepts of consistency and availability dictate your choice between a strongly consistent relational database and an eventually consistent NoSQL store?**
If the domain requires absolute correctness at all times to prevent catastrophic logical errors (e.g., financial ledgers deducting balances), strict consistency (RDBMS) is mandatory despite latency costs. If the domain prioritizes uptime, speed, and massive scale (e.g., user activity logs, prompt history), eventual consistency (NoSQL) provides better availability and resilience.