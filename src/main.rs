use std::sync::Arc;
use tokio::sync::Mutex;

mod producer;
mod consumer;
mod source;
mod broker;

use consumer::delivery_consumer;
use fluvio::spu::SpuSocketPool;
use producer::delivery_producer;
use tokio::task;

struct ZomatoProducers {
    pub delivery_producer: fluvio::TopicProducer<SpuSocketPool>,
    pub efficiency_producer: fluvio::TopicProducer<SpuSocketPool>,
    pub customer_satisfaction_producer: fluvio::TopicProducer<SpuSocketPool>,
    pub vehicle_performance_producer: fluvio::TopicProducer<SpuSocketPool>,
}

impl ZomatoProducers {
    pub async fn new() -> Self {
        let delivery_producer = fluvio::producer("4-test").await.unwrap();
        let efficiency_producer = fluvio::producer("1-test").await.unwrap();
        let customer_satisfaction_producer = fluvio::producer("2-test").await.unwrap();
        let vehicle_performance_producer = fluvio::producer("3-test").await.unwrap();
        ZomatoProducers {
            delivery_producer,
            efficiency_producer,
            customer_satisfaction_producer,
            vehicle_performance_producer,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let zomato_producers = Arc::new(Mutex::new(ZomatoProducers::new().await));

    // Clone the Arc for the spawned task
    let producer_arc = Arc::clone(&zomato_producers);

    // Spawn a separate task to run the producer in a loop
    task::spawn(async move {
        let producers = producer_arc.lock().await;
        // This is the main delivery producer thread which will
        // constantly keep listening for all incoming delivery data
        if let Err(e) = delivery_producer(&producers.delivery_producer).await {
            eprintln!("Producer encountered an error: {:?}", e);
        }
    });

    // Delivery Consumer will listen to all incoming Delivery Data
    delivery_consumer(zomato_producers).await?;
    Ok(())
}
