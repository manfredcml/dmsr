Design Document: DMSR (Data Migration Service in Rust)
====================================

## Introduction

DMSR, also known as Data Migration Service in Rust, is an open-source tool that allows users to replicate data between
different storage systems. Inspired by AWS DMS, which offers a similar service under a paid, proprietary model, DMSR
enables users to specify a source (typically a database) and a target (databases, object storage systems, or message
queues) for replication. The framework supports both one-off and continuous replication, allowing users to replicate
data in real-time or on-demand.

## Architecture

The architecture of DMSR is modular and consists of key components that work together to replicate data between storage
systems. These components include:

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
