"""
✅ UPDATED Fake Data Readers
Updated to use verified 2016 schemas
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from schemas.data_schemas import *
from datetime import datetime, timedelta
import random

class FakeDataReader:
    """
    Fake reader tạo sample data cho testing
    Interface giống real reader để dễ swap sau
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_weather_data(self, start_date=None, end_date=None, num_records=1000):
        """
        FAKE: Tạo weather data (long format)
        REAL: Sẽ đọc từ Kafka hoặc HDFS
        """
        print(f"📊 [FAKE] Generating {num_records} weather records...")
        
        data = []
        base_date = datetime(2016, 1, 1)
        
        for i in range(num_records):
            record = {
                "datetime": base_date + timedelta(hours=i),
                "city": "New York",
                "temperature": random.uniform(260, 310),  # Kelvin
                "humidity": random.uniform(30, 95),
                "pressure": random.uniform(1000, 1025),
                "wind_speed": random.uniform(0, 15),
                "wind_direction": random.uniform(0, 360),
                "weather_description": random.choice(["clear sky", "few clouds", "rain", "snow", "thunderstorm"]),
                "rain_1h": random.uniform(0, 5) if random.random() > 0.7 else 0.0,
                "snow_1h": random.uniform(0, 3) if random.random() > 0.9 else 0.0,
                "clouds_all": random.randint(0, 100),
            }
            data.append(record)
        
        df = self.spark.createDataFrame(data, schema=weather_schema)
        print(f"   ✅ Generated {df.count()} weather records")
        return df
    
    def read_311_requests(self, start_date=None, end_date=None, num_records=500):
        """
        FAKE: Tạo 311 service request data
        REAL: Sẽ đọc từ Kafka hoặc HDFS
        """
        print(f"📊 [FAKE] Generating {num_records} 311 service requests...")
        
        data = []
        base_date = datetime(2016, 1, 1)
        boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
        complaint_types = [
            "Noise - Street/Sidewalk", "Heat/Hot Water", "Blocked Driveway",
            "Illegal Parking", "Street Condition", "Water System",
            "Tree Damage", "Sewer", "Flooded Street"
        ]
        
        for i in range(num_records):
            borough = random.choice(boroughs)
            created = base_date + timedelta(hours=random.randint(0, 8760))
            
            record = {
                "unique_key": f"REQ_{i+1:06d}",
                "created_date": created,
                "closed_date": created + timedelta(days=random.randint(1, 30)),
                "agency": random.choice(["NYPD", "DEP", "DOT"]),
                "agency_name": "New York City Police Department",
                "complaint_type": random.choice(complaint_types),
                "descriptor": "Service Request",
                "location_type": "Street/Sidewalk",
                "incident_zip": str(random.randint(10001, 11697)),
                "incident_address": f"{random.randint(1, 999)} MAIN ST",
                "street_name": "MAIN STREET",
                "city": "NEW YORK",
                "borough": borough,
                "latitude": 40.7128 + random.uniform(-0.2, 0.2),
                "longitude": -74.0060 + random.uniform(-0.2, 0.2),
                "status": random.choice(["Closed", "Open", "Pending"]),
                "due_date": created + timedelta(days=random.randint(1, 7)),
                "resolution_description": "Resolved" if random.random() > 0.5 else None,
                "community_board": f"0{random.randint(1, 12)}",
            }
            data.append(record)
        
        df = self.spark.createDataFrame(data, schema=service_311_schema)
        print(f"   ✅ Generated {df.count()} 311 requests")
        return df
    
    def read_taxi_trips(self, start_date=None, end_date=None, num_records=800):
        """
        FAKE: Tạo taxi trip data (2016 schema with lat/lon)
        REAL: Sẽ đọc từ Kafka hoặc HDFS
        """
        print(f"📊 [FAKE] Generating {num_records} taxi trips...")
        
        data = []
        base_date = datetime(2016, 1, 1)
        
        for i in range(num_records):
            pickup_time = base_date + timedelta(minutes=random.randint(0, 525600))
            trip_duration = timedelta(minutes=random.randint(5, 60))
            
            record = {
                "VendorID": random.choice([1, 2]),
                "tpep_pickup_datetime": pickup_time,
                "tpep_dropoff_datetime": pickup_time + trip_duration,
                "passenger_count": random.randint(1, 6),
                "trip_distance": random.uniform(0.5, 20.0),
                "pickup_longitude": -73.98 + random.uniform(-0.1, 0.1),
                "pickup_latitude": 40.75 + random.uniform(-0.1, 0.1),
                "RatecodeID": 1,
                "store_and_fwd_flag": "N",
                "dropoff_longitude": -73.98 + random.uniform(-0.1, 0.1),
                "dropoff_latitude": 40.75 + random.uniform(-0.1, 0.1),
                "payment_type": random.choice([1, 2]),
                "fare_amount": random.uniform(5, 50),
                "extra": 0.5,
                "mta_tax": 0.5,
                "tip_amount": random.uniform(0, 10),
                "tolls_amount": random.uniform(0, 5) if random.random() > 0.8 else 0,
                "improvement_surcharge": 0.3,
                "total_amount": random.uniform(6, 70),
            }
            data.append(record)
        
        df = self.spark.createDataFrame(data, schema=taxi_trip_schema_2016)
        print(f"   ✅ Generated {df.count()} taxi trips")
        return df
    
    def read_collisions(self, start_date=None, end_date=None, num_records=300):
        """
        FAKE: Tạo collision data
        REAL: Sẽ đọc từ Kafka hoặc HDFS
        """
        print(f"📊 [FAKE] Generating {num_records} collision records...")
        
        data = []
        base_date = datetime(2016, 1, 1).date()
        boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
        factors = [
            "Driver Inattention/Distraction", "Failure to Yield Right-of-Way",
            "Following Too Closely", "Unsafe Speed",
            "Rain", "Snow", "Slippery Road", "Fog"
        ]
        vehicles = ["Sedan", "SUV", "Taxi", "Truck", "Bus", "Motorcycle"]
        
        for i in range(num_records):
            crash_date = base_date + timedelta(days=random.randint(0, 365))
            
            record = {
                "collision_id": f"COLL_{i+1:08d}",
                "crash_date": crash_date,
                "crash_time": f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}",
                "borough": random.choice(boroughs),
                "zip_code": str(random.randint(10001, 11697)),
                "latitude": 40.7128 + random.uniform(-0.2, 0.2),
                "longitude": -74.0060 + random.uniform(-0.2, 0.2),
                "location": f"(40.{random.randint(6000, 8000)}, -74.{random.randint(0, 2000)})",
                "on_street_name": f"STREET {random.randint(1, 200)}",
                "cross_street_name": f"AVENUE {random.randint(1, 50)}",
                "off_street_name": f"BUILDING {random.randint(1, 100)}" if random.random() > 0.7 else None,
                "number_of_persons_injured": random.randint(0, 5),
                "number_of_persons_killed": 1 if random.random() > 0.95 else 0,
                "number_of_pedestrians_injured": random.randint(0, 2),
                "number_of_pedestrians_killed": 0,
                "number_of_cyclist_injured": random.randint(0, 1),
                "number_of_cyclist_killed": 0,
                "number_of_motorist_injured": random.randint(0, 3),
                "number_of_motorist_killed": 0,
                "contributing_factor_vehicle_1": random.choice(factors),
                "contributing_factor_vehicle_2": random.choice(factors) if random.random() > 0.5 else None,
                "vehicle_type_code_1": random.choice(vehicles),
                "vehicle_type_code_2": random.choice(vehicles) if random.random() > 0.5 else None,
            }
            data.append(record)
        
        df = self.spark.createDataFrame(data, schema=collision_schema)
        print(f"   ✅ Generated {df.count()} collision records")
        return df


# ========================================
# REAL DATA READERS (Template)
# ========================================

class RealDataReader:
    """
    Template cho real readers - implement sau khi có Kafka/HDFS
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_from_kafka(self, topic, bootstrap_servers="localhost:9092"):
        """TODO: Implement Kafka reader"""
        df = self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .load()
        return df
    
    def read_from_hdfs(self, path, format="parquet"):
        """TODO: Implement HDFS reader"""
        if format == "parquet":
            df = self.spark.read.parquet(path)
        elif format == "csv":
            df = self.spark.read.csv(path, header=True)
        return df
