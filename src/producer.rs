use fluvio_protocol::record::RecordKey;
use tokio::time;

use crate::source::read_csv;

pub async fn produce_records() -> anyhow::Result<()> {
    // Bring source data into the producer
    let filename = "src/zomato.csv";
    let soure_data = read_csv(filename).unwrap();

    let producer = fluvio::producer("cnbc-test").await?;

    for data in soure_data {
        producer.send(RecordKey::NULL, format!("{:#?}", data)).await?;
        producer.flush().await?;
        time::sleep(time::Duration::from_secs(5)).await;
    }
    Ok(())
}
