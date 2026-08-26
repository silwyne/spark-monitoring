# Spark Monitoring with Prometheus + Grafana

This repository demonstrates how to monitor a Docker Compose deployed Apache Spark cluster using **Prometheus** and **Grafana**.

The project provides a complete local monitoring environment containing:

* Apache Spark standalone cluster
* Spark application execution example
* Prometheus metrics collection
* Grafana visualization dashboard

The goal is to provide visibility into Spark performance metrics such as memory usage, JVM metrics, executor behavior, and streaming/application-level statistics.

---

## Architecture

```
                    +----------------+
                    |   Grafana      |
                    |  Dashboard     |
                    +-------+--------+
                            |
                            |
                    +-------v--------+
                    |  Prometheus    |
                    | Metrics Store  |
                    +-------+--------+
                            |
                            |
              +-------------v--------------+
              |        Spark Cluster       |
              |                            |
              |  +----------------------+  |
              |  |   Spark Master       |  |
              |  +----------------------+  |
              |             |              |
              |  +----------------------+  |
              |  |   Spark Worker       |  |
              |  +----------------------+  |
              |                            |
              |  +----------------------+  |
              |  |   Spark Job Driver   |  |
              |  +----------------------+  |
              +----------------------------+
```

---

## Components

### Apache Spark

The cluster is deployed using Docker Compose with:

* Spark Master
* Spark Worker
* Spark Job container

The Spark containers share the same configuration directory:

```
spark/configs/
```

The Spark job runs using:

```
spark-submit
```

against the standalone Spark master.

---

### Prometheus

Prometheus is responsible for:

* Scraping Spark metrics
* Storing time-series data
* Providing PromQL queries for Grafana

Prometheus configuration:

```
prometheus/prometheus.yml
```

Prometheus UI:

```
http://localhost:9090
```

---

### Grafana

Grafana provides visualization dashboards for Spark metrics.

Grafana is available at:

```
http://localhost:3000
```

Default credentials:

```
username: grafana
password: grafana
```

A ready-to-import dashboard is included:

```
spark-prometheus-dashboard.json
```

---

## Repository Structure

```
.
├── docker-compose.yml
├── spark-prometheus-dashboard.json
│
├── prometheus/
│   └── prometheus.yml
│
└── spark/
    ├── code/
    │   └── replicator.py
    │
    ├── configs/
    │
    └── dependencies/
```

---

## Requirements

Before running the stack, install:

* Docker
* Docker Compose

Verify installation:

```bash
docker --version
docker compose version
```

---

## Running the Monitoring Stack

Clone the repository:

```bash
git clone https://github.com/silwyne/spark-monitoring.git

cd spark-monitoring
```

Start all services:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

Expected services:

```
spark-master
spark-worker
spark-job
prometheus
grafana
```

---

## Access Services

| Service         | URL                   |
| --------------- | --------------------- |
| Spark Master UI | http://localhost:8080 |
| Spark Driver UI | http://localhost:4040 |
| Spark Worker UI | http://localhost:8082 |
| Prometheus      | http://localhost:9090 |
| Grafana         | http://localhost:3000 |

---

## Import Grafana Dashboard

1. Open Grafana:

```
http://localhost:3000
```

2. Login:

```
username: grafana
password: grafana
```

3. Go to:

```
Dashboards → Import
```

4. Upload:

```
spark-prometheus-dashboard.json
```

5. Select Prometheus as the datasource.

After importing, the dashboard provides Spark monitoring views.

---

## Monitoring Metrics

The dashboard includes panels for metrics such as:

### JVM Metrics

* Driver JVM heap memory
* Executor JVM memory usage

### BlockManager Metrics

* Used memory
* Maximum memory
* Remaining memory
* On-heap memory usage
* Off-heap memory usage

### Spark Streaming Metrics

* State store rows
* Streaming related counters

---

## Useful Commands

View logs:

```bash
docker compose logs -f
```

View logs for a specific service:

```bash
docker compose logs -f spark-job
```

Restart the environment:

```bash
docker compose restart
```

Stop everything:

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

---

## Configuration

### Spark Configuration

Spark settings can be modified inside:

```
spark/configs/
```

These files are mounted into Spark containers at:

```
/opt/spark/conf
```

---

### Prometheus Configuration

Prometheus scraping configuration:

```
prometheus/prometheus.yml
```

Modify this file to add additional Spark targets or monitoring jobs.

---

## Purpose

This project is useful for:

* Learning Spark observability
* Testing Spark workloads locally
* Understanding Spark performance metrics
* Building a lightweight monitoring environment
* Experimenting with Prometheus and Grafana dashboards

---

## Future Improvements

Possible extensions:

* Add Alertmanager for alerting
* Add Node Exporter for host metrics
* Add cAdvisor for container metrics
* Add Kubernetes deployment manifests
* Add automated Grafana datasource provisioning
* Add production-ready persistent storage configuration

---

## License

This project is provided for educational and experimental purposes.
