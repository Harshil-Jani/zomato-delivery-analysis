use fluvio::{
    consumer::{ ConsumerConfigExtBuilder, ConsumerStream, OffsetManagementStrategy },
    Fluvio,
    Offset,
};
use futures_util::StreamExt;

pub async fn run_consumer() -> anyhow::Result<()> {
    let fluvio = &Fluvio::connect().await?;
    let mut stream = fluvio.consumer_with_config(
        ConsumerConfigExtBuilder::default()
            .topic("cnbc-test".to_string())
            .offset_consumer("cnbc-test".to_string())
            .offset_start(Offset::beginning())
            .offset_strategy(OffsetManagementStrategy::Manual)
            .build()?
    ).await?;

    while let Some(Ok(record)) = stream.next().await {
        println!("{}", analyze_sentiment(&String::from_utf8_lossy(record.as_ref())));
        stream.offset_commit()?;
        stream.offset_flush().await?;
    }
    Ok(())
}

// A simple function to analyze sentiment (for demonstration purposes)
fn analyze_sentiment(headline: &str) -> String {
    if headline.contains("a") {
        return "Harshil, ".to_string();
    }
    return "AA".to_string();
}
