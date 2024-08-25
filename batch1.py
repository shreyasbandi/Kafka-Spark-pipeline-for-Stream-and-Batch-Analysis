from pymongo import MongoClient
import time

# Initialize MongoDB client
client = MongoClient("mongodb://localhost:27017/")
db = client["dbt_c"]  # Replace "your_database" with your actual database name
collection = db["raw_data"]    # Collection where raw data is stored

# Define temperature ranges and corresponding labels
temperature_ranges = [(0, 10), (10, 20), (20, float('inf'))]
temperature_labels = ['Cold', 'Medium', 'Hot']

# Initialize dictionaries to store total turbidity and entry count for each temperature range
total_turbidity = {label: 0 for label in temperature_labels}
entry_count = {label: 0 for label in temperature_labels}

# Query MongoDB for all documents in the collection
start_time = time.time()
cursor = collection.find()

# Iterate over the documents and categorize them based on temperature ranges
for document in cursor:
    temp = document["temperature"]
    turbidity = document["turbidity"]
    
    # Find the appropriate temperature range and update total turbidity and count
    for label, (temp_min, temp_max) in zip(temperature_labels, temperature_ranges):
        if temp >= temp_min and temp < temp_max:
            total_turbidity[label] += turbidity
            entry_count[label] += 1
            break

# Calculate average turbidity for each temperature range
average_turbidity = {label: total_turbidity[label] / entry_count[label] if entry_count[label] != 0 else 0 for label in temperature_labels}

# Print the average turbidity for each temperature range
for label in temperature_labels:
    print(f"Average Turbidity for {label} Temperature Range:", average_turbidity[label])

# Calculate and print the time taken
end_time = time.time()
elapsed_time = end_time - start_time
print("Time taken:", elapsed_time, "seconds")

