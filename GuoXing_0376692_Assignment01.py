import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder

# ==========================
# ==== Load the dataset ====
# ==========================
data = pd.read_csv(r"D:\OneDrive\桌面\data.csv")

# ===================================
# ==== Exploratory Data Analysis ====
# ===================================
print("Get data dtype for each column:")
print(data.dtypes)
print()

print("Check basic information about the dataset:")
print(data.info())
print()

print("Get summary statistics for numeric columns:")
print(data.describe())
print()

print("Listing the basic information of all columns:")
print(data.head())
print()

# ============================
# ==== Data Preprocessing ====
# ============================
# Identifying Missing Values
print("Check the number of missing values for all columns:")
print(data.isnull().sum().sort_values(ascending=False).head(20))
print()

# Handling Missing Values
columns = data.columns[data.isnull().all()].tolist()
if 'flag' in columns:
	data.drop(columns=['flag'], inplace=True) # Drop the column that is empty

missed_cate_cols = ['wbincome2023']
missed_num_cols = ['estimate', 'se', 'ci_ub', 'ci_lb', 'population']

numeric_imputer = SimpleImputer(strategy='median')
categorical_imputer = SimpleImputer(strategy='constant', fill_value='missing')

data[missed_num_cols] = numeric_imputer.fit_transform(data[missed_num_cols])
data[missed_cate_cols] = categorical_imputer.fit_transform(data[missed_cate_cols])

# Verifying Missing Values
print("Check missing values still exist after cleaned data:")
print(data.isnull().sum().head(20))
print()

# Outlier Detection
numeric_cols = list(data.select_dtypes(include=[np.number]).columns)
def iqr_quantile(data, cols=None):
	q1 = data[cols].quantile(0.25)
	q3 = data[cols].quantile(0.75)
	iqr = q3 - q1
	lower_bound = q1 - 1.5 * iqr
	upper_bound = q3 + 1.5 * iqr
	mask = (data[cols] < lower_bound) | (data[cols] > upper_bound)
	return mask, lower_bound, upper_bound

mask, lower_bound, upper_bound = iqr_quantile(data, numeric_cols)
print("Detected Outliers:")
print(mask.sum().sort_values(ascending=False))
print()

# Remove outliers
data_clipped = data.copy()
data_clipped[numeric_cols] = data_clipped[numeric_cols].clip(lower=lower_bound, upper=upper_bound, axis=1)

# Check outliers after clipping
new_mask, _, _ = iqr_quantile(data_clipped, numeric_cols)
print("Check outliers after clipping:")
print(new_mask.sum())
print()

data = data_clipped

# =============================
# ==== Data Transformation ====
# =============================
# Data standardization
min_max_scaler = MinMaxScaler()
data[numeric_cols] = min_max_scaler.fit_transform(data[numeric_cols])

# Remain key features
key_numeric_features = ['se', 'ci_lb', 'ci_ub']

for k in key_numeric_features:
	if k in numeric_cols:
		numeric_cols.remove(k)

print("Scaled data:")
print(data[numeric_cols])

# Encoding categorical data
categorical_cols = list(data.select_dtypes(include=["object", "category"]).columns)

# Remain key features
key_categorical_features = ['setting', 'date', 'source', 'update']
target_col = "estimate"

for col in [target_col] + key_categorical_features:
	try:
		categorical_cols.remove(col)
	except ValueError:
		pass

le_dict = {}
for col in categorical_cols:
	le = LabelEncoder()
	data[col] = le.fit_transform(data[col].astype(str))
	le_dict[col] = le

print("Encoded data:")
print(data[categorical_cols])

leakage_cols = ['ci_lb', 'ci_ub', 'se']

# Information Leakage
for col in [target_col] + leakage_cols:
    if col in numeric_cols:
        numeric_cols.remove(col)

feature_cols = numeric_cols + categorical_cols

x = data[feature_cols]
y = data[target_col]

# Splitting the dataset
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42, shuffle=True)

# =============================
# ==== Feature Engineering ====
# =============================
# Time-series analysis
data['date'] = pd.to_datetime(data['date'], errors='coerce')
data.sort_values('date', inplace=True)

date_monthly = data.resample('ME').mean(numeric_only=True)

# Compute average rolling
data['Rolling_Mean'] = data['estimate'].shift(1).rolling(window=3).mean()

# Compute first-order difference
data['Diff'] = data['estimate'].diff()

# Feature selection
gbr = GradientBoostingRegressor(n_estimators=200, max_depth=8, random_state=42)
gbr.fit(x_train, y_train)
fi = pd.Series(gbr.feature_importances_, index=x_train.columns)
print("Feature importances related to 'estimate':")
print(fi.sort_values(ascending=False))

# ====== Heat map visualization ======
feat_imp_df = fi.sort_values(ascending=False).reset_index()
feat_imp_df.columns = ['feature', 'importance']

plt.figure(figsize = (8,len(feat_imp_df) * 0.4))
sns.heatmap(data=feat_imp_df[['importance']], annot=True, cmap='coolwarm', fmt='.2f',
            yticklabels=feat_imp_df['feature'], cbar=True, linewidths=0.5)

plt.title('Feature Importance Heatmap', fontsize=25)
plt.xlabel('Importance Value', fontsize =20)
plt.ylabel('Feature Name', fontsize =20)
plt.tight_layout()
plt.show()

# Save cleaned dataset
data_to_save = data.reset_index() if isinstance(data.index, pd.DatetimeIndex) else data.copy()
data_to_save.to_csv(r"D:\OneDrive\桌面\cleaned_data.csv", index=False, encoding='utf-8')
print("Saved cleaned dataset to: D:\\OneDrive\\桌面\\cleaned_data.csv")

# =============================================
# ==== Use PySpark for Big Data Processing ====
# =============================================
# Configure path
input_path = r"D:\OneDrive\桌面\cleaned_data.csv"
out_partitioned_path = r"D:\OneDrive\桌面\out_parquet"

# Create sparkSession
spark = SparkSession.builder.appName("Pandas_Spark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read CSV file as spark DataFrame
data_spark = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Convert date format
data_spark = data_spark.withColumn("date", F.to_date(F.col("date"), "yyyy-MM"))

# Reduce the volume of data by Filtering
data_spark_filtered = data_spark.dropna(subset=["setting", "date"])

# Cache intermediate results to avoid double counting
data_spark_filtered.cache()

# Repartition by partition column to improve write performance
data_spark_partitioned = data_spark_filtered.repartition("setting", "date")

# Import parquet
data_spark_partitioned.write \
    .option("maxRecordsPerFile", 45000) \
    .partitionBy("setting", "date") \
    .mode("overwrite") \
    .parquet(out_partitioned_path)

# Release Cache
data_spark_filtered.unpersist()

spark.stop()