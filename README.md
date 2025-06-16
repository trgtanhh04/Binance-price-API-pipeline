# Real-time Bitcoin Price Analytics Pipeline

## 1. Introduction

This project implements a real-time data pipeline for collecting, processing, and analyzing Bitcoin price data from Binance. The architecture uses Kafka for data streaming, Spark Structured Streaming for real-time analytics, and MongoDB for persistent storage. The pipeline is broken down into Extract, Transform, and Load (ETL) stages, with an optional bonus analysis.

---

## 2. Architecture Overview

**Stages:**
1. **Extract:** Collects BTC price data from Binance API and publishes to Kafka.
2. **Transform:** 
   - Computes moving averages and standard deviations over various sliding windows, publishes to Kafka.
   - Computes Z-scores for each price against the moving statistics, publishes to Kafka.
3. **Load:** Reads Z-score data from Kafka and writes to MongoDB.
4. **Bonus:** Finds the shortest window after each price record where the price increases or decreases.

---
## 3. Workflow

<p align="center">
<img src="https://raw.githubusercontent.com/trgtanhh04/Binance-price-API-pipeline/main/images/pipeline.png" width="100%" alt="airflow">
</p>

---

## 3. Stage Details

### 3.1. Extract

- **Functionality:**
  - Fetches the current BTC price from Binance API:  
    `https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT`
  - Adds an ISO8601 event-time timestamp (rounded based on chosen frequency).
  - Runs at least once per 100 milliseconds.
  - Publishes records to Kafka topic `btc-price`.

- **Kafka Message Example:**
    ```json
    {
      "symbol": "BTCUSDT",
      "price": 68400.00,
      "timestamp": "2025-06-16T12:32:10.400Z"
    }
    ```

- **Technical Notes:**
  - Any programming language can be used.
  - Timestamp precision depends on crawling frequency.

---

### 3.2. Transform

#### 3.2.1. Moving Average & Standard Deviation

- **Functionality:**
  - Reads from Kafka topic `btc-price`.
  - Uses event-time to group messages into sliding windows: `30s`, `1m`, `5m`, `15m`, `30m`, `1h`.
  - Calculates average and standard deviation of prices per window.
  - Handles late data (up to 10 seconds).
  - Publishes results to Kafka topic `btc-price-moving`.

- **Output Example:**
    ```json
    {
      "timestamp": "2025-06-16T12:32:10.400Z",
      "symbol": "BTCUSDT",
      "windows": [
        {
          "window": "30s",
          "avg_price": 68380.0,
          "std_price": 30.5
        },
        ...
      ]
    }
    ```

#### 3.2.2. Z-score Calculation

- **Functionality:**
  - Reads from Kafka topics `btc-price` and `btc-price-moving`.
  - Joins records on timestamp, computes Z-score for each price/window:
    - `zscore = (price - avg_price) / std_price`
  - Handles edge cases (e.g., stddev = 0).
  - Publishes to Kafka topic `btc-price-zscore`.

- **Output Example:**
    ```json
    {
      "timestamp": "2025-06-16T12:32:10.400Z",
      "symbol": "BTCUSDT",
      "zscores": [
        {
          "window": "30s",
          "zscore_price": 1.32
        },
        ...
      ]
    }
    ```

---

### 3.3. Load

- **Functionality:**
  - Reads from Kafka topic `btc-price-zscore`.
  - Writes results to MongoDB collections, one per window:
    - `btc-price-zscore-30s`
    - `btc-price-zscore-1m`
    - etc.
  - Each document typically contains:
    - `timestamp` (datetime)
    - `symbol` (string)
    - `window` (string)
    - `zscore_price` (float)

- **Technology:** Spark Structured Streaming, MongoDB Spark Connector.

---

### 3.4. Bonus: Shortest Increasing/Decreasing Price Window

- **Functionality:**
  - Reads records from `btc-price`.
  - For each record at time t, finds within (t, t+20s]:
    - The first message with a higher price (for "higher" window).
    - The first message with a lower price (for "lower" window).
    - If none found, outputs 20.0s as placeholder.
  - Publishes to Kafka topics:
    - `btc-price-higher`
    - `btc-price-lower`

- **Output Example:**
    ```json
    {
      "timestamp": "2025-06-16T12:32:10.400Z",
      "higher_window": 3.2
    }
    ```
    or
    ```json
    {
      "timestamp": "2025-06-16T12:32:10.400Z",
      "lower_window": 20.0
    }
    ```

- **Note:** Use stateful stream processing as needed.

---

## 4. Technologies Used

- **Kafka:** For real-time data streaming.
- **Spark Structured Streaming:** For windowed/statistical processing.
- **MongoDB:** For persistent storage.
- **Binance API:** As data source.

---

## 5. How to Run

1. **Start Kafka and MongoDB.**
2. **Run the Extract producer (fetches from Binance and pushes to Kafka).**
3. **Run the Transform Spark jobs (process and publish to Kafka).**
4. **Run the Load Spark job (writes Z-score to MongoDB).**
5. **(Bonus) Run the Spark job for shortest increasing/decreasing price window.**

---

## 6. Screenshots & Explanations

Include key screenshots in your report, such as:
- MongoDB Atlas user and IP setup.
- Kafka topic creation and monitoring.
- Producer logs showing data push.
- Spark UI during streaming jobs.
- MongoDB Compass displaying collections.
- (Add detailed explanations per screenshot.)

---

## 7. References

- [Binance Market Data API](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [MongoDB Documentation](https://www.mongodb.com/docs/manual/introduction/)
- [Z-score - Wikipedia](https://en.wikipedia.org/wiki/Standard_score)

---

> **Note:**  
> - Log and screenshot every significant step for your report.
> - If you encounter problems with code or configuration, debug each step separately.
