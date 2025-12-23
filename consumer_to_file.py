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

def create_file_consumer():
	consumer = KafkaConsumer(
		TOPIC_NAME,
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		group_id="file_group_v1",
		value_deserializer=lambda v: json.loads(v.decode("utf-8-sig")),
		consumer_timeout_ms=20000
	)
	return consumer


def process_and_save():
	consumer = create_file_consumer()
	header = ("date, setting, source, indicator_abbr, indicator_name, dimension, subgroup, estimate,"
			  "se, ci_lb, ci_ub, population, setting_average, iso3, favourable_indicator, indicator_scale,"
			  "ordered_dimension, subgroup_order, reference_subgroup, whoreg6, wbincome2023,"
			  "update, dataset_id\n")
	
	with open(OUTPUT_FILE, "w", encoding="utf-8-sig") as f:
		f.write(header)
	
	count = 0
	
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
		
		line = f"{date}, {setting}, {source}, {indicator_abbr}, {indicator_name}, {dimension}, {subgroup}, {estimate}," \
			   f"{se}, {ci_lb}, {ci_ub}, {population}, {setting_average}, {iso3}, {favourable_indicator}, " \
			   f"{indicator_scale},{ordered_dimension}, {subgroup_order}, {reference_subgroup}, {whoreg6}," \
			   f"{wbincome2023}, {update}, {dataset_id}\n"
		
		print("Processed:", line.strip())
		
		count += 1
		if count % 20000 == 0:
			print(f"File saved {count} rows.")
		
	with open(OUTPUT_FILE, "a", encoding="utf-8-sig") as f:
		f.write(line)
	
	print(f"Successfully processed {count} rows to {OUTPUT_FILE}.")

if __name__ == "__main__":
	process_and_save()