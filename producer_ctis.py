import csv, json
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
	
	try:
		with open(csv_path, newline='', encoding="utf-8-sig") as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				producer.send(topic, value=row)
				
				count += 1
				if count % 20000 == 0:
					print(f"Sent {count} rows")
	
	except Exception as e:
		print(f"Error: {e}")
	finally:
		producer.flush()
		producer.close()
		print(f"All data has been sent. In total {count} rows")

if __name__ == "__main__":
	stream_orders()