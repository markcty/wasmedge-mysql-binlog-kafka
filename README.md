# wasmedge-mysql-binlog-kafka-per-table

## Implementation

### Tables to be processed

Firstly, I hard coded a vec into the code to represent the tables whose binlogs needs to be processed.
```Rust
const TABLES: &[&str] = &["paymentA", "paymentB", "paymentC"];
```

### Create a topic for each table
As described in the requirements, the event for each table needs to be sent to a specific kafka topic. So, before starting to replicate binlogs to Kafka, we create a topic for each table. The `LogEventClient` contains all the information needed to replicate the binlogs for a table, including the topic name, partition client, and the kafka offset.
```Rust
// create topics for each table
let mut clients: HashMap<String, LogEventClient> = HashMap::new();
for table in TABLES {
    let topic = format!("{mysql_database}_{table}");
    kafka_producer.create_topic(&topic).await;
    let partition_client = kafka_producer.get_partition_client(0).await.unwrap();
    let offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
    clients.insert(
        table.to_string(),
        LogEventClient {
            topic,
            client: partition_client,
            offset,
        },
    );
}
```

### Receiving binlog events from `BinlogClient`
Like before, we receive binlog events from the BinlogClient and convert the events to json.
```Rust
for result in binlog_client.replicate()? {
    let (header, event) = result?;
    let json_event = serde_json::to_string(&event).expect("Couldn't convert sql event to json");
    let json_header =
        serde_json::to_string(&header).expect("Couldn't convert sql header to json");

    // process events...

    binlog_client.commit(&header, &event);
}
```

### Extract table information from the binlog event
The `BinlogEvent` doesn't contain table information. We must determine which table(s) the event belongs to. We can do this by parsing the inner sql statement using the `sqlparser` library.

For demonstration purpose, I only take a limited conditions into considerations. Only sqls like `CREATE TABLE xxx`, `SELECT xxx FROM xxx`, and `INSERT xxx INTO xxx` will be correctly recognized.
```Rust
let tables = get_event_table(&event);

fn get_event_table(event: &BinlogEvent) -> HashSet<String> {
    match event {
        BinlogEvent::QueryEvent(e) => extract_tables_from_sql(&e.sql_statement),
        _ => HashSet::new(),
    }
}

fn extract_tables_from_sql(sql: &str) -> HashSet<String> {
    let ast = Parser::parse_sql(&MySqlDialect {}, sql).expect("failed to parse sql");
    let mut tables = HashSet::new();
    for st in ast {
        match st {
            Statement::Insert { table_name, .. } => {
                for table in table_name.0 {
                    tables.insert(table.value);
                }
            }
            Statement::CreateTable { name, .. } => {
                for table in name.0 {
                    tables.insert(table.value);
                }
            }
            Statement::Query(q) => {
                if let sqlparser::ast::SetExpr::Select(s) = *q.body {
                    for f in s.from {
                        if let sqlparser::ast::TableFactor::Table { name, .. } = f.relation {
                            for table in name.0 {
                                tables.insert(table.value);
                            }
                        }
                    }
                }
            }

            _ => {}
        }
    }
    tables
}
```


### Produce and consume the log record to relevant kafka topics
Now that we know which tables the event belongs to, we can write the record in the corresponding kafka topic. Then, we consume these log records and print it out.

```Rust
for table in tables {
    // The event for tables not needed will be skipped
    let Some(client) = clients.get_mut(&table) else {
        continue;
    };

    client
        .produce_log_record(json_header.clone(), json_event.clone())
        .await;

    let records = client.consume_log_records().await;

    // print record
    for r in records {
        println!(
            "===================Event from kafka topic {}===================",
            client.topic
        );
        // ...
    }
}
```

The code for producing and consuming log records(much like before):
```Rust
impl LogEventClient {
    async fn produce_log_record(&mut self, header: String, event: String) {
        let record = Record {
            key: None,
            value: Some(event.into_bytes()),
            headers: BTreeMap::from([("mysql_binlog_headers".to_owned(), header.into_bytes())]),
            timestamp: Utc.timestamp_millis(42),
        };
        self.client
            .produce(vec![record], Compression::default())
            .await
            .expect("failed to produce");
    }

    async fn consume_log_records(&mut self) -> Vec<Record> {
        let (records, high_watermark) = self
            .client
            .fetch_records(
                self.offset, // offset
                1..100_000,  // min..max bytes
                1_000,       // max wait time
            )
            .await
            .unwrap();

        self.offset = high_watermark;
        records.into_iter().map(|ro| ro.record).collect()
    }
}
```

## Test

### Test code
Test code is similar to the original `insert.wasm`, except that we now insert the payment records into multiple tables rather than one table. The code is in `mysql-binlog-kafka/src/sql-commands-test-rust/insert_tables.rs`

```Rust
const TABLES: &[&str] = &["paymentA", "paymentB", "paymentC"];

for table in TABLES {
    // create table and insert records...
}
```

### Test result
To verify my implementation does the job, the github ci file is changed to:
```yml
- name: Run tests
run: |
    cd mysql-binlog-kafka
    wasmedge --env "DATABASE_URL=mysql://root:password@127.0.0.1:3306/mysql" sql-commands-test-wasm/insert_tables.wasm
    sleep 10
    cat out.log | grep "Event from kafka topic mysql_paymentA" && echo "Query executed successfully" || exit 1
    cat out.log | grep "Event from kafka topic mysql_paymentB" && echo "Query executed successfully" || exit 1
    cat out.log | grep "Event from kafka topic mysql_paymentC" && echo "Query executed successfully" || exit 1
```
The ci will pass if the event is sent to the corresponding kafka topic.

And it does!

![ci](assets/ci.png)
