import csv, json, time
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# ==== Basic configuration ====
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "CTIS-dataset"
DATA_FILE = r"D:\OneDrive\桌面\Cleaned CTIS data.csv"
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

def create_producer():
	producer = KafkaProducer(
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8-sig"),
		key_serializer=lambda k: k.encode("utf-8-sig") if isinstance(k, str) else None,
		acks=1,
		compression_type="gzip",
		batch_size=65536,
		linger_ms=20,
		buffer_memory=33554432
	)
	return producer

def stream_orders(csv_path=DATA_FILE, topic=TOPIC_NAME):
	producer = create_producer()
	count = 0
	total_latency = 0
	stats = {"total_latency_msr": 0}
	
	# Start count
	start_time = time.time()
	print("Waiting for messages...")
	
	# Start calculate latency
	def on_success(record_metadata, start_msg_time):
		latency = (time.time() - start_msg_time) * 1000
		stats["total_latency_msr"] += latency
	
	try:
		with open(csv_path, newline='', encoding="utf-8-sig") as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				msg_send_start = time.time()
				row["sent_at"] = datetime.now(timezone.utc).isoformat(timespec="microseconds")
				
				# Use callback to track
				producer.send(topic, value=row).add_callback(on_success, msg_send_start)
				
				count += 1
				if count % 20000 == 0:
					elapsed = time.time() - start_time
					print(f"Sent {count} rows. Speed: {count / elapsed:.2f} rows/sec")
	
	except Exception as e:
		print(f"Error: {e}")
	finally:
		producer.flush()
		producer.close()
		
		# Compute and print throughput rate
		end_time = time.time()
		final_elapsed = end_time - start_time
		throughput = count / final_elapsed
		
		if count > 0 and final_elapsed > 0:
			avg_latency = stats["total_latency_msr"] / count
			print(f"All Data has been sent successfully, in total {count} rows")
			print(f"Total Time consumption: {final_elapsed:.2f} seconds")
			print(f"Producer throughput: {throughput:.2f} rows/sec")
			print(f"Producer average latency: {avg_latency:.2f} ms")
		else:
			print("Consumer throughput: 0 rows/sec (No data received)")

if __name__ == "__main__":
	stream_orders()