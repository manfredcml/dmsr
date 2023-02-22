Design Document: DMSR (Data Migration System in Rust)
====================================

## Introduction

DMSR, or Data Migration System in Rust, is a tool designed to enable users to replicate data between different data
storage systems. The idea is inspired by AWS DMS, which serves the same purpose yet under a paid
proprietary service provided AWS. DMSR allows users to specify a source and a target for data replication, where the
source is primarily a database and the target can be databases, object storage systems or message queues. The framework
supports both one-off and continuous data replication, allowing users to replicate data on-demand or in real-time.

The design principles of the data framework are:

- Container-native: Designed to be deployed in containers, orchestrated by tools such as Kubernetes.
- Cloud-agnostic: Not specific to any cloud provider.
- Open-source: Free to use and modify.
- Extensible: Support for new data sources and targets can be added easily.
- Flexible: Users can specify the replication mode and the replication frequency.
- Reliable: Data replication is reliable and fault-tolerant.
- Efficient: Data replication is efficient and scalable.
- Secure: Data replication is secure and encrypted.
- Simple: Easy to use and configure.

## Architecture

DMSR is designed as a modular system composed of several key components that work together to replicate
data between different storage systems. The main components of the framework are:

### Data Sources and Targets

The data sources represent the storage systems from which data will be replicated, while the data targets represent the
storage systems to which data will be replicated. Both data sources and data targets can be implemented using an
abstract interface provided by the data framework.

### Streamer

A messaging system that connects the data sources and data targets. The streamer is responsible for receiving data
from the data sources, transforming it as necessary, and writing it to the data targets. The streamer is also
responsible
for ensuring that the data is replicated reliably and efficiently. By default, the streamer uses Kafka as the messaging
system, but it can be configured to use other messaging systems as long as it complies with the interface.

## Interfaces

TODO

## Comparison against existing tools

| Feature             | DMSR                   | AWS DMS                | Debezium                   |
|---------------------|------------------------|------------------------|----------------------------|
| Data Source Support | Yes                    | Yes                    | Yes                        |
| Data Target Support | Yes                    | Yes                    | Yes                        |
| Streaming Support   | Yes                    | Yes                    | Yes                        |
| Kafka Integration   | Yes                    | Yes                    | Yes                        |
| Replication Modes   | One-off and Continuous | One-off and Continuous | Continuous                 |
| Deployment          | Kubernetes             | Cloud or On-premises   | Cloud or On-premises       |
| Extensibility       | Yes                    | No                     | Yes                        |
| Transformation      | Customizable           | Customizable           | Customizable               |
| Cost                | Open-source            | Pay-per-use or BYOL    | Open-source or Pay-per-use |

## Example use (Local)

TODO

## Example use (Kubernetes)

TODO
