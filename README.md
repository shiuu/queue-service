# Message Queues

This project is an implementation of message queue with basic features.

## Background

Message queues are a ubiquitous mechanism for achieving horizontal scalability.
However, many production message services (e.g., Amazon's SQS) do not come with
an offline implementation suitable for local development and testing.  The purpose
of this project is to resolve this deficiency by designing a simple
message-queue API that supports three implementations:

 - an in-memory queue, suitable for same-JVM producers and consumers. The in-memory queue is thread safe.
 - a file-based queue, suitable for same-host producers and consumers, but
   potentially different JVMs. The file-based queue is thread safe and inter-process safe when run
  in a *nix environment.
 - an adapter for a production queue service, such as SQS.

The intended usage is that application components be written to use queues via
the common interface (QueueService), and injected with an instance suitable for the environment
in which that component is running (development, testing, integration-testing,
staging, production, etc).

## Main Features of Message Queue
 - multiplicity
   
   A queue supports many producers and many consumers.

 - delivery
   
   A queue strives to deliver each message exactly once to exactly one consumer,
   but guarantees at-least once delivery (it can re-deliver a message to a
   consumer, or deliver a message to multiple consumers, in some cases).

 - order
   
   A queue strives to deliver messages in FIFO order, but makes no guarantee
   about delivery order.

 - reliability
   
   When a consumer receives a message, it is not removed from the queue.
   Instead, it is temporarily suppressed (becomes "invisible").  If the consumer
   that received the message does not subsequently delete it within a
   timeout period (the "visibility timeout"), the message automatically becomes
   visible at the head of the queue again, ready to be delivered to another
   consumer.


## Code Structure

The code under the com.example package.
1. QueueService.java: the interface to cater for the essential queue actions:
   - push     pushes a single message onto a specified queue
   - pull     receives a single message from a specified queue
   - delete   deletes a received message

2. InMemoryQueueService.java: an in-memory version of QueueService. The in-memory queue is thread-safe.

3. FileQueueService.java: implement a file-based version of the interface,
   which uses file system to co-ordinate between producers and consumers in
   different JVMs (i.e. thread-safe in a single VM, but also inter-process safe
   when used concurrently in multiple VMs).

4. SqsQueueService.java: a SQS-based version of the interface.

5. Config file: src/main/resources/config.properties.

6. Unit tests (including test the behavior of the visibility timeout).

## Building and Running
You can use Maven to run tests from the command-line with:
  mvn package
