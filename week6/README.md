## Week 5 Overview

* [DE Zoomcamp 6.3 - What is kafka?](#de-zoomcamp-63---what-is-kafka)

## [DE Zoomcamp 6.3 - What is kafka?](https://www.youtube.com/watch?v=zPLZUDPi4AY)

**Topic:** a topic is a continuous stream of events, where an event refers to a single data point collected at a specific timestamp. The collection of a sequence of events goes into a topic and is read by a consumer. In Kafka, events are stored as logs in a topic (i.e., how the data is stored inside a topic) and each event contains a message.

**Message:** a message is a structure that stores a key, a value and a timestamp. The key is a field that is an identifier used in the data mining process and is also useful for tasks such as partitioning. The value is the data that is being exchanged. The timestamp indicates the exact moment that the message was produced.

**Kafka:** is a platform that provides robustness and reliability to topics when building a streaming system, by replicating data across different nodes and tolerating faults such as some nodes going down. It also provides scalability and flexibility, being able to handle thousands of events per second, topics of different sizes and a varying number of consumers.

