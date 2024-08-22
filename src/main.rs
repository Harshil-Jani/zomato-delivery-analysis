mod producer;
mod consumer;
mod source;

use consumer::run_consumer;
use producer::produce_records;
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Spawn a separate task to run the producer in a loop
    task::spawn(async {
        produce_records().await.unwrap();
    }).await?;

    // Run the consumer in the main thread
    run_consumer().await?;
    Ok(())
}
