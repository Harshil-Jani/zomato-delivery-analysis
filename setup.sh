#!/bin/bash

# Start the Fluvio cluster
fluvio cluster start

# Check if the previous command failed
if [ $? -ne 0 ]; then
    echo "Fluvio cluster start failed. Attempting to resume the cluster..."
    fluvio cluster resume

    # Check if the resume command also fails
    if [ $? -ne 0 ]; then
        echo "Fluvio cluster resume failed. Exiting script."
        exit 1
    else
        echo "Fluvio cluster successfully resumed."
    fi
else
    echo "Fluvio cluster started successfully."
fi

# Create topics
fluvio topic create delivery
fluvio topic create efficiency-analysis
fluvio topic create customer-satisfaction-analysis
fluvio topic create vehicle-condition-analysis
