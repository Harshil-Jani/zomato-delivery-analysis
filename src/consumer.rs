use std::sync::Arc;

use fluvio::{
    consumer::{ ConsumerConfigExtBuilder, ConsumerStream, OffsetManagementStrategy },
    Fluvio,
    Offset,
};
use futures_util::StreamExt;

use crate::{ broker::run_broker, source::ZomatoData, ZomatoProducers };

pub async fn delivery_consumer(
    zomato_producers: Arc<ZomatoProducers>
) -> anyhow::Result<()> {
    println!("Delivery Consumer Connected");
    let fluvio = &Fluvio::connect().await?;
    let mut stream = fluvio.consumer_with_config(
        ConsumerConfigExtBuilder::default()
            .topic("delivery".to_string())
            .offset_consumer("delivery".to_string())
            .offset_start(Offset::beginning())
            .offset_strategy(OffsetManagementStrategy::Manual)
            .build()?
    ).await?;

    while let Some(Ok(record)) = stream.next().await {
        let data = record.get_value().as_str().unwrap();
        // Deserialize data back to ZomatoData struct
        let delivery_data: ZomatoData = serde_json::from_str::<ZomatoData>(data).unwrap();
        // This data is further sent to broker which manages three producers which will then pass on to consumers.
        run_broker(&zomato_producers, delivery_data).await.unwrap();
    }
    stream.offset_commit()?;
    stream.offset_flush().await?;
    Ok(())
}
