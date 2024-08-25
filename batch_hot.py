from pymongo import MongoClient

# Initialize MongoDB client
client = MongoClient("mongodb://localhost:27017/")
db = client["dbt"]  # Replace "your_database" with your actual database name
collection = db["hot"]    # Collection where raw data is stored

# Define a function to calculate the overall average turbidity
def calculate_overall_avg_turbidity():
    total_turbidity = 0
    entry_count = 0
    
    # Query MongoDB for all documents in the collection
    cursor = collection.find()
    
    # Iterate over the documents and calculate total turbidity and count
    for document in cursor:
        total_turbidity += document["turbidity"]
        entry_count += 1
    
    # Calculate the overall average turbidity
    overall_avg_turbidity = total_turbidity / entry_count if entry_count != 0 else 0
    return overall_avg_turbidity

# Calculate the overall average turbidity for all data in the collection
overall_avg_turbidity = calculate_overall_avg_turbidity()
print("Overall Average Turbidity:", overall_avg_turbidity)

