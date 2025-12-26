import csv, json, mysql.connector, time
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

def create_mysql_consumer():
	consumer = KafkaConsumer(
		TOPIC_NAME,
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		group_id="mysql-group-v3",
		value_deserializer=lambda v: json.loads(v.decode("utf-8-sig")),
		consumer_timeout_ms=20000,
	)
	return consumer

def process_and_insert():
	consumer = create_mysql_consumer()
	conn = mysql.connector.connect(**DB_CONFIG)
	cursor = conn.cursor()
	
	count = 0
	total_latency_ms = 0
	batch_buffer = []
	# Start count
	start_time = time.time()
	
	try:
		if conn.is_connected():
			print("Successfully connected to MySQL, start batch warehousing...")
			
			sql = """
                          INSERT INTO ctis_processed (`date`, setting, source, indicator_abbr, indicator_name,
                                                      dimension, subgroup, estimate, se, ci_lb, ci_ub, population,
                                                      setting_average, iso3, favourable_indicator, indicator_scale,
                                                      ordered_dimension, subgroup_order, reference_subgroup, whoreg6,
                                                      wbincome2023, `update`, dataset_id)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s)
			"""
			
			for message in consumer:
				row = message.value
				now = datetime.now(timezone.utc)
				
				# Start calculate latency
				if "sent_at" in row:
					sent = datetime.fromisoformat(row["sent_at"])
					latency = (now - sent).total_seconds() * 1000
					total_latency_ms += latency
				
				values = (
					row.get("date") or row.get("\ufeffdate"),
					row.get("setting"),row.get("source"),row.get("indicator_abbr"),row.get("indicator_name"),
					row.get("dimension"),row.get("subgroup"),row.get("estimate"),row.get("se"),row.get("ci_lb"),
					row.get("ci_ub"),row.get("population"),row.get("setting_average"),row.get("iso3"),
					row.get("favourable_indicator"),row.get("indicator_scale"),row.get("ordered_dimension"),
					row.get("subgroup_order"),row.get("reference_subgroup"),row.get("whoreg6"),row.get("wbincome2023"),
					row.get("update"),row.get("dataset_id")
				)
				
				batch_buffer.append(values)
				count += 1
				
				if count % 20000 == 0:
					cursor.executemany(sql, batch_buffer)
					conn.commit()
					batch_buffer = []
					elapsed = time.time() - start_time
					print(f"Processed data has been stored {count} rows. Speed: {count / elapsed:.2f} rows/sec")
				
	except Exception as e:
		print(f"Error: {e}")
	
	end_time = time.time()
	final_elapsed = end_time - start_time
	
	if count > 0 and final_elapsed > 0:
		avg_latency = total_latency_ms / count
		print(f"All Data has been stored successfully, in total {count} rows")
		print(f"Total Time consumption: {final_elapsed:.2f} seconds")
		print(f"SQL connector throughput: {count / final_elapsed:.2f} rows/sec")
		print(f"SQL consumer average latency: {avg_latency:.2f} ms")
	else:
		print("SQL connector throughput: 0 rows/sec (No data received)")
		
if __name__ == "__main__":
	process_and_insert()