# Zomato Delivery Analysis

An event driven architecture for the Zomato Ops team that could help in analysis of driver logistics with respect to weather, traffic, vehicle type etc.

# Application Architecture

![Architecture of the Zomato Operational Pipelines](image.png)

This project demonstrates an event-driven architecture using Fluvio for streaming data analysis. The architecture is composed of several key components:

1. **Data Source**:
   The data source is the origin of the real-time event data. In this case, the data source sends information upon the completion of a delivery. This data contains various details relevant to the delivery, such as delivery ID, delivery person details, order details, environmental conditions, and delivery time.
2. **Event**:
   The event triggers the flow of data into the system. It signifies that new delivery data is available and ready to be processed.
3. **Fluvio Producer**:
   The Fluvio producer is responsible for continuously streaming incoming data from the event into the Fluvio system. The producer listens for new delivery data and publishes it to the appropriate Fluvio topics for further analysis.
4. **Analysis**:
   The analysis component consumes the incoming data from the producer. It processes the delivery data and performs various analyses, generating different types of results based on the data. This could include metrics like delivery efficiency, customer satisfaction, or environmental impact.
5. **Broker**:
   The broker manages different topics within Fluvio where the analysis results are published. Each type of analysis result is sent to a specific topic, ensuring that consumers can subscribe to and process the data they are interested in.
6. **Fluvio Consumers**:
   The Fluvio consumers are the endpoints that subscribe to specific Fluvio topics to receive and process the analysis results.
   Consumer 1 subscribes to the first type of analysis result, processing data from Fluvio Topic - 1.
   Consumer 2 subscribes to the second type of analysis result, processing data from Fluvio Topic - 2.
   Consumer 3 subscribes to the third type of analysis result, processing data from Fluvio Topic - 3.
7. **Fluvio Topics**:
   The topics within Fluvio act as channels where the processed analysis results are sent. Each topic corresponds to a specific kind of analysis result, and consumers can subscribe to these topics to receive relevant data.
