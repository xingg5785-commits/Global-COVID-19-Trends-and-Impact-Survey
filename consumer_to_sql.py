import csv, json, mysql.connector
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
	
	try:
		if conn.is_connected():
			print("Successfully connected to MySQL.")
			
			columns_sql = """
                          INSERT INTO ctis_processed (`date`, setting, source, indicator_abbr, indicator_name,
                                                      dimension, subgroup, estimate, se, ci_lb, ci_ub, population,
                                                      setting_average, iso3, favourable_indicator, indicator_scale,
                                                      ordered_dimension, subgroup_order, reference_subgroup, whoreg6,
                                                      wbincome2023, `update`, dataset_id)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s)
						  """
			
			for message in consumer:
				data = message.value
				date = data.get("date") or data.get("\ufeffdate")
				setting = data.get("setting")
				source = data.get("source")
				indicator_abbr = data.get("indicator_abbr")
				indicator_name = data.get("indicator_name")
				dimension = data.get("dimension")
				subgroup = data.get("subgroup")
				estimate = data.get("estimate")
				se = data.get("se")
				ci_lb = data.get("ci_lb")
				ci_ub = data.get("ci_ub")
				population = data.get("population")
				setting_average = data.get("setting_average")
				iso3 = data.get("iso3")
				favourable_indicator = data.get("favourable_indicator")
				indicator_scale = data.get("indicator_scale")
				ordered_dimension = data.get("ordered_dimension")
				subgroup_order = data.get("subgroup_order")
				reference_subgroup = data.get("reference_subgroup")
				whoreg6 = data.get("whoreg6")
				wbincome2023 = data.get("wbincome2023")
				update = data.get("update")
				dataset_id = data.get("dataset_id")
				
				values = (date, setting, source, indicator_abbr, indicator_name, dimension, subgroup, estimate,
						  se, ci_lb, ci_ub, population, setting_average, iso3, favourable_indicator, indicator_scale,
						  ordered_dimension, subgroup_order, reference_subgroup, whoreg6, wbincome2023,
						  update, dataset_id)
				
				cursor.execute(columns_sql, values)
				conn.commit()
				print("Inserted into DB:", values)
				
				count += 1
				if count % 20000 == 0:
					conn.commit()
					print(f"Progress: {count} rows inserted.")
	
	except Exception as e:
		print(f"Error: {e}")
		
	finally:
		if conn and conn.is_connected():
			cursor.close()
			conn.close()
		consumer.close()
		print("Connection closed.")

if __name__ == "__main__":
	process_and_insert()