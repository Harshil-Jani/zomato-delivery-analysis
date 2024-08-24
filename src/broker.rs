use std::sync::Arc;
use fluvio::{ spu::SpuSocketPool, RecordKey, TopicProducer };

use crate::{ source::ZomatoData, ZomatoProducers };

pub async fn run_broker(
    zomato_producers: &Arc<ZomatoProducers>,
    data: ZomatoData
) -> anyhow::Result<()> {
    println!("Broker received some event");
    // This broker will collect all the data received from the delivery consumer.
    // This data will be further sent to three different producers.
    // Each producer will then send this data to a different consumer.
    prepare_efficiency_analysis_data(&zomato_producers.efficiency_producer, &data).await;
    prepare_customer_satisfaction_data(&zomato_producers.customer_satisfaction_producer, &data).await;
    prepare_vehicle_performance_data(&zomato_producers.vehicle_performance_producer, &data).await;
    Ok(())
}

async fn prepare_customer_satisfaction_data(
    producer: &TopicProducer<SpuSocketPool>,
    data: &ZomatoData
) {
    // Simple model: satisfaction decreases with longer delivery times, but increases with higher ratings
    // Satisfaction score calculation
    let base_satisfaction = 10.0 - (data.time_taken_min as f32) / 10.0;
    let rating_factor = data.delivery_person_ratings / 2.0;
    let satisfaction_score =
        base_satisfaction + rating_factor + (if data.festival { -0.5 } else { 0.0 });

    let mut analyzed_data = serde_json::to_value({}).unwrap();
    analyzed_data["order_id"] = serde_json::json!(data.id);
    analyzed_data["satisfaction_score"] = serde_json::json!(satisfaction_score);
    analyzed_data["rating_factor"] = serde_json::json!(rating_factor);
    analyzed_data["base_satisfaction"] = serde_json::json!(base_satisfaction);

    let serialized_data = serde_json::to_string(&analyzed_data).unwrap();
    producer.send(RecordKey::NULL, serialized_data).await.unwrap();
    producer.flush().await.unwrap();
}

async fn prepare_vehicle_performance_data(
    producer: &TopicProducer<SpuSocketPool>,
    data: &ZomatoData
) {
    // Vehicle performance score calculation
    let base_performance = data.vehicle_condition as f32;
    let vehicle_type_factor = match data.type_of_vehicle.as_str() {
        "motorcycle" => 1.5,
        "scooter" => 1.0,
        "electric_scooter" => 1.2,
        _ => 1.0, // Default case
    };
    let multiple_deliveries_factor = if data.multiple_deliveries > 0 { -0.5 } else { 0.0 };
    let performance_score = base_performance * vehicle_type_factor + multiple_deliveries_factor;

    let mut analyzed_data = serde_json::to_value({}).unwrap();
    analyzed_data["order_id"] = serde_json::json!(data.id);
    analyzed_data["performance_score"] = serde_json::json!(performance_score);
    analyzed_data["vehicle_type_factor"] = serde_json::json!(vehicle_type_factor);
    analyzed_data["multiple_deliveries_factor"] = serde_json::json!(multiple_deliveries_factor);
    analyzed_data["base_performance"] = serde_json::json!(base_performance);

    // Serialize required data format to a JSON string.
    let serialized_data = serde_json::to_string(&analyzed_data).unwrap();
    producer.send(RecordKey::NULL, serialized_data).await.unwrap();
    producer.flush().await.unwrap();
}

async fn prepare_efficiency_analysis_data(
    producer: &TopicProducer<SpuSocketPool>,
    data: &ZomatoData
) {
    // Efficiency score calculation
    let base_efficiency = 10.0 - (data.time_taken_min as f32) / 5.0;
    let traffic_factor = match data.road_traffic_density.as_str() {
        "Low" => 2.0,
        "Medium" => 1.0,
        "High" => 0.0,
        _ => 1.0, // Default case
    };
    let weather_factor = match data.weather_conditions.as_str() {
        "Sunny" => 1.0,
        "Cloudy" => 0.5,
        "Foggy" => -0.5,
        "Sandstorms" => -1.0,
        _ => 0.0, // Default case
    };
    let efficiency_score = base_efficiency + traffic_factor + weather_factor;

    let mut analyzed_data = serde_json::to_value({}).unwrap();
    analyzed_data["order_id"] = serde_json::json!(data.id);
    analyzed_data["efficiency_score"] = serde_json::json!(efficiency_score);
    analyzed_data["traffic_factor"] = serde_json::json!(traffic_factor);
    analyzed_data["weather_factor"] = serde_json::json!(weather_factor);
    analyzed_data["base_efficiency"] = serde_json::json!(base_efficiency);

    // Serialize required data format to a JSON string.
    let serialized_data = serde_json::to_string(&analyzed_data).unwrap();
    producer.send(RecordKey::NULL, serialized_data).await.unwrap();
    producer.flush().await.unwrap();
}
