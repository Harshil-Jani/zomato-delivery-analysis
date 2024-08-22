use std::error::Error;
use std::fs::File;
use std::path::Path;

#[derive(Debug)]
pub struct ZomatoData {
    pub id: String,
    pub delivery_person_id: String,
    pub delivery_person_age: u8,
    pub delivery_person_ratings: f32,
    pub order_date: String,
    pub time_ordered: String, // Keep String for now
    pub time_order_picked: String, // Keep String for now
    pub weather_conditions: String,
    pub road_traffic_density: String,
    pub vehicle_condition: u8,
    pub type_of_order: String,
    pub type_of_vehicle: String,
    pub multiple_deliveries: u8,
    pub festival: bool,
    pub city: String,
    pub time_taken_min: u8,
}

pub fn read_csv<P: AsRef<Path>>(filename: P) -> Result<Vec<ZomatoData>, Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut data: Vec<ZomatoData> = Vec::new();
    for result in rdr.records() {
        let record = result?;
        if record[13].parse::<u8>().is_err() {
            println!("Error parsing field vehicle_condition {}", record[13].to_string());
            continue;
        }
        // If record 13 is "Yes" then mark true else false
        let festival = if &record[17] == "Yes" { true } else { false };
        if record[19].parse::<u8>().is_err() {
            println!("Error parsing field time_taken_min {:#?}", record[19].to_string());
            continue;
        }
        if record[16].parse::<u8>().is_err() {
            println!("Error parsing field multiple_deliveries {:#?}", record[16].to_string());
            continue;
        }
        if record[2].parse::<u8>().is_err() {
            println!("Error parsing field delivery_person_age {:#?}", record[2].to_string());
            continue;
        }

        let row = ZomatoData {
            id: record[0].to_string(),
            delivery_person_id: record[1].to_string(),
            delivery_person_age: record[2].parse().unwrap(),
            delivery_person_ratings: record[3].parse().unwrap(),
            order_date: record[8].to_string(),
            time_ordered: record[9].to_string(),
            time_order_picked: record[10].to_string(),
            weather_conditions: record[11].to_string(),
            road_traffic_density: record[12].to_string(),
            vehicle_condition: record[13].parse().unwrap(),
            type_of_order: record[14].to_string(),
            type_of_vehicle: record[15].to_string(),
            multiple_deliveries: record[16].parse().unwrap(),
            festival: festival,
            city: record[18].to_string(),
            time_taken_min: record[19].parse().unwrap(),
        };

        data.push(row);
    }
    println!("Done reading csv");
    Ok(data)
}
