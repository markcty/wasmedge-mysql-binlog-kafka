use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;

use chrono::{TimeZone, Utc};
use rskafka::client::partition::{OffsetAt, PartitionClient};
use rskafka::client::Client;
use rskafka::{
    client::{
        partition::{Compression, UnknownTopicHandling},
        ClientBuilder,
    },
    record::Record,
};
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::{thread, time::Duration};

const TABLES: &[&str] = &["paymentA", "paymentB", "paymentC"];

struct LogEventClient {
    topic: String,
    client: PartitionClient,
    offset: i64,
}

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

struct KafkaProducer {
    client: Client,
    topic: Option<String>,
}

impl KafkaProducer {
    async fn connect(url: String) -> Self {
        KafkaProducer {
            client: ClientBuilder::new(vec![url])
                .build()
                .await
                .expect("Couldn't connect to kafka"),
            topic: None,
        }
    }

    async fn create_topic(&mut self, topic_name: &str) {
        let topics = self.client.list_topics().await.unwrap();

        for topic in topics {
            if topic.name.eq(&topic_name.to_string()) {
                self.topic = Some(topic_name.to_string());
                println!("Topic already exist in Kafka");
                return;
            }
        }

        let controller_client = self
            .client
            .controller_client()
            .expect("Couldn't create controller client kafka");
        controller_client
            .create_topic(
                topic_name, 1,     // partitions
                1,     // replication factor
                5_000, // timeout (ms)
            )
            .await
            .unwrap();
        self.topic = Some(topic_name.to_string());
    }

    async fn get_partition_client(&self, partition: i32) -> Option<PartitionClient> {
        let topic = self.topic.as_ref().unwrap();
        Some(
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .expect("Couldn't fetch controller client"),
        )
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), mysql_cdc::errors::Error> {
    let sleep_time: u64 = std::env::var("SLEEP_TIME").unwrap().parse().unwrap();

    thread::sleep(Duration::from_millis(sleep_time));
    println!("Thread started");

    // // Start replication from MariaDB GTID
    // let _options = BinlogOptions::from_mariadb_gtid(GtidList::parse("0-1-270")?);
    //
    // // Start replication from MySQL GTID
    // let gtid_set =
    //     "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
    // let _options = BinlogOptions::from_mysql_gtid(GtidSet::parse(gtid_set)?);
    //
    // // Start replication from the position
    // let _options = BinlogOptions::from_position(String::from("mysql-bin.000008"), 195);
    //
    // Start replication from last master position.
    // Useful when you are only interested in new changes.
    let options = BinlogOptions::from_end();

    // Start replication from first event of first available master binlog.
    // Note that binlog files by default have expiration time and deleted.
    // let options = BinlogOptions::from_start();

    let username = std::env::var("SQL_USERNAME").unwrap();
    let password = std::env::var("SQL_PASSWORD").unwrap();
    let mysql_port = std::env::var("SQL_PORT").unwrap();
    let mysql_hostname = std::env::var("SQL_HOSTNAME").unwrap();

    let mysql_database = std::env::var("SQL_DATABASE").unwrap();
    let options = ReplicaOptions {
        username,
        password,
        port: mysql_port.parse::<u16>().unwrap(),
        hostname: mysql_hostname,
        database: Some(mysql_database.clone()),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);
    println!("Connected to mysql database");

    let kafka_url = std::env::var("KAFKA_URL").unwrap();
    let mut kafka_producer = KafkaProducer::connect(kafka_url).await;
    println!("Connected to kafka server");

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

    for result in client.replicate()? {
        let (header, event) = result?;
        let json_event = serde_json::to_string(&event).expect("Couldn't convert sql event to json");
        let json_header =
            serde_json::to_string(&header).expect("Couldn't convert sql header to json");

        let tables = get_event_table(&event);
        for table in tables {
            let Some(client) = clients.get_mut(&table) else {
                continue;
            };
            // produce and consume
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
                println!();
                println!(
                    "Value: {}",
                    String::from_utf8(r.value.expect("record has no value")).unwrap()
                );
                println!("Timestamp: {}", r.timestamp);
                println!(
                    "Headers: {}",
                    String::from_utf8(
                        r.headers
                            .get("mysql_binlog_headers")
                            .expect("record has no mysql_binlog_headers")
                            .to_owned()
                    )
                    .unwrap()
                );
                println!();
                println!();
            }
        }

        // After you processed the event, you need to update replication position
        client.commit(&header, &event);
    }
    Ok(())
}

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
