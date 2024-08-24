use fluvio::{ spu::SpuSocketPool, TopicProducer };
use fluvio_protocol::record::RecordKey;
use tokio::time;

use crate::source::read_csv;

pub async fn delivery_producer(producer: &TopicProducer<SpuSocketPool>) -> anyhow::Result<()> {
    println!("Connected to Delivery Producer");
    // Bring source data into the producer
    // In real-time data, This should be an API call every few seconds.
    // or could be a streaming socket connection which can be looped to constantly
    // listen to incoming data.
    let filename = "src/zomato.csv";
    let soure_data = read_csv(filename).unwrap();
    for data in soure_data {
        let serialized_data = serde_json::to_string(&data).unwrap();
        producer.send(RecordKey::NULL, serialized_data).await?;
        producer.flush().await?;
        time::sleep(time::Duration::from_secs(5)).await;
    }
    Ok(())
}
