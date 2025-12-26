import csv, json, time
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# ==== Basic configuration ====
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "CTIS-dataset"
OUTPUT_FILE = r"D:\OneDrive\桌面\ETL CTIS data.csv"
DB_CONFIG = {
	"host": "localhost",
	"user": "root",
	"password": "g12345",
	"database": "ctis_db",
	"auth_plugin": "mysql_native_password"
}

COLUMNS = [
	"date", "setting", "source", "indicator_abbr", "indicator_name", "dimension",
	"subgroup", "estimate", "se", "ci_lb", "ci_ub", "population", "setting_average",
	"iso3", "favourable_indicator", "indicator_scale", "ordered_dimension",
	"subgroup_order", "reference_subgroup", "whoreg6", "wbincome2023", "update", "dataset_id"
]

def create_file_consumer():
	consumer = KafkaConsumer(
		TOPIC_NAME,
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		group_id="file_group_v2",
		value_deserializer=lambda v: json.loads(v.decode("utf-8-sig")),
		consumer_timeout_ms=20000
	)
	return consumer

def process_and_save():
	consumer = create_file_consumer()
	count = 0
	total_latency_ms = 0
	
	# Start count
	start_time = time.time()
	print("Waiting for messages...")
	
	try:
		with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline='', buffering=65536) as f:
			writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction='ignore')
			writer.writeheader()
			
			for message in consumer:
				row = message.value
				now = datetime.now(timezone.utc)
				
				# Start calculate latency
				if "sent_at" in row:
					sent = datetime.fromisoformat(row["sent_at"])
					latency = (now - sent).total_seconds() * 1000
					total_latency_ms += latency

					date = row.get("date") or row.get("\ufeffdate")
					
				writer.writerow(row)
				
				count += 1
				if count % 20000 == 0:
					elapsed = time.time() - start_time
					print(f"Processed {count} rows. Speed: {count / elapsed:.2f} rows/sec")
			
	except Exception as e:
		print(f"Error during processing: {e}")
		
	end_time = time.time()
	final_elapsed = end_time - start_time
	
	if count > 0 and final_elapsed > 0:
		avg_latency = total_latency_ms / count
		print(f"All Data has been processed successfully, in total {count} rows")
		print(f"Total Time Consumption: {final_elapsed:.2f} seconds")
		print(f"Consumer throughput: {count / final_elapsed:.2f} rows/sec")
		print(f"Consumer average latency: {avg_latency:.2f} ms")
	else:
		print("Consumer throughput: 0 rows/sec (No data received)")

if __name__ == "__main__":
	process_and_save()