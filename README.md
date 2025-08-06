## Intro
This project implements a streaming data pipeline to analyze browser history. A producer microservice reads URLs from a CSV file and sends them as individual messages to a Kafka topic. A separate processor application consumes these messages, calculates domain visit statistics in real-time, and prints a summary of the top five most visited domains.
## Link to a source code repository with the implementation of microservices
https://github.com/xphoenixua/data_streaming_kafka_hw3
## Instructions on how to run the implementation
1. Make sure there is `history.csv` file in the root. 
2. Then start the app:
   `docker compose up --build -d`
3. Check `processor-1` service's logs for the statistics. 
4. To stop the app:
   `docker compose down -v`

## Pipeline details
The initial approach for this task might be a simple producer-consumer model. In that pattern, I would write a consumer with an explicit `for` loop that manually pulls messages from Kafka. Inside this loop, I would manage the state myself, like creating and updating a Python `Counter` object to store domain counts, and then periodically printing the results.

Instead, I implemented a solution based on the principles of Kafka Streams using the Bytewax framework. This approach is declarative rather than imperative. I defined a dataflow topology, which is a graph of processing steps. This is a higher-level abstraction where I describe *what* should happen to the data, and the framework handles the complex underlying work of looping, managing state, and ensuring data flows correctly between steps.
### Bytewax dataflow implementation
- `kop.input`

  I started the dataflow by connecting to the `browser_history` Kafka topic. This operator handles all the details of creating a consumer and fetching messages. It produces a stream of raw messages.
- `op.filter_map`

  I chose this operator for efficiency. It combines mapping and filtering into one step. The `get_domain` function attempts to parse each message and extract a valid domain. If it fails or if a domain is not present, it returns `None`, and `filter_map` automatically discards that item from the stream.
- `op.key_on`

  The windowing operator needs to know how to group items. Since I wanted a global count of all domains across all messages, I used `op.key_on` to assign a single, static key (namely, "ALL") to every single domain. This forces all items into the same processing group.
- `win.collect_window`

  I used a `TumblingWindower` to create 10-second, non-overlapping "buckets" of time. The `collect_window` operator then gathers all the domains that arrive within each 10-second window into a list. This approach is fundamental to stream processing, as it allows for analyzing an infinite stream by breaking it into finite chunks.
- `op.map(format_output)`

  After a window is closed, it emits its result, which is a list of all domains seen in that time frame. I then used a final map step to take this list, calculate the visit counts using a `Counter`, find the top five, and format the results into a human-readable string.
- `op.output`

  The final formatted string is sent to a `StdOutSink`, which simply prints it to the console.
