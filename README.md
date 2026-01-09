## Real-Time Log Analytics & Incident Detection System

### Overview

A real-time streaming analytics system built using **Apache Flink (PyFlink)** and **Apache Kafka** to monitor application logs, detect operational anomalies, and generate automated incident alerts.

The system processes high-volume log streams, applies **event-time window aggregations**, evaluates **error rates and latency SLOs**, and publishes alerts to Kafka for downstream consumers such as dashboards, alerting systems, or databases.

---

### Key Features

* Real-time log ingestion using Apache Kafka
* Event-time processing with watermarking
* Sliding and tumbling window aggregations in PyFlink
* Error rate spike detection
* P95 latency SLO breach monitoring
* Stateful alert suppression to avoid false positives
* Kafka-based alert pipeline for decoupled consumers

---

### Architecture

```
Log Producers
      ↓
Kafka (Raw Logs)
      ↓
PyFlink Streaming Jobs
  - Error Rate Detector
  - Latency SLO Monitor
      ↓
Kafka (Incident Alerts)
      ↓
Dashboards / Alerting Systems / Storage
```

---

### Streaming Jobs

#### 1. Error Rate Detector

* Uses **sliding event-time windows**
* Calculates error rate per service
* Triggers alerts when error rate exceeds threshold
* Designed for continuous anomaly detection

#### 2. Latency SLO Monitor

* Uses **tumbling event-time windows**
* Computes P95 latency per service
* Tracks consecutive SLO breaches using state
* Triggers critical alerts after repeated violations

---

### Technologies Used

* Apache Flink (PyFlink)
* Apache Kafka
* Event-Time Processing & Watermarks
* Stateful Stream Processing
* Python
* Docker (for local setup)


Just say the word 🚀
