import textwrap, sys
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
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

# ===========================
# ==== Data Distribution ====
# ===========================
data['population'] = np.log1p(data['population'])
core_num_cols = ['estimate', 'population', 'se', 'setting_average']

# Draw the data distribution of numeric columns
plt.figure(figsize = (15, 6))
for i, col in enumerate(core_num_cols):
    plt.subplot(1, 4, i+1)
    sns.histplot(data[col].dropna(), kde=True)
    plt.title(f'Distribution of {col}')

plt.tight_layout()
plt.show()

# Draw the data distribution of categorical columns
core_cate_cols = ['whoreg6', 'wbincome2023', 'dimension']

fig, axes = plt.subplots(3, 1, figsize=(12, 15), gridspec_kw={'height_ratios': [1, 1, 1.5]})

# Draw the first three plots
for i, col in enumerate(core_cate_cols):
	curr_order = data[col].value_counts().index
	
	sns.countplot(data=data, y=col, ax=axes[i], order=curr_order)
	axes[i].set_title(f"Distribution of {col} (Sorted)", fontsize=16)
	axes[i].set_ylabel("")

plt.tight_layout()
plt.show()

# Draw forth plot
data['indicator_wrapped'] = data['indicator_name'].apply(lambda x: textwrap.fill(x, width=45))

indicator_order = data['indicator_wrapped'].value_counts().index

plt.figure(figsize = (18, 20))
sns.countplot(data=data, y='indicator_wrapped', order=indicator_order)
plt.title("Distribution of indicator_name", fontsize=16)
plt.xlabel("Count", fontsize=10)
plt.tick_params(axis='both', labelsize=10)
plt.ylabel("")

plt.tight_layout()
plt.show()

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

# Output the dataset for PowerBi analysis
# data_for_powerbi = data_clipped
# data_for_powerbi.to_csv(r"D:\OneDrive\桌面\Data for Powerbi.csv", index=False, encoding="utf-8-sig")
# print("Saved dataset to: D\\OneDrive\\桌面\\Data for Powerbi.csv")
# sys.exit(0)

# =========================================================
# ==== Data Transformation (Used for Machine Learning) ====
# =========================================================
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
data = data.dropna(subset=['date'])

data = data.sort_values(by=['date', 'setting'])

data = data.set_index('date').sort_index()

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
print()

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

# Save cleaned dataset for machine learning
data_to_save = data.reset_index() if isinstance(data.index, pd.DatetimeIndex) else data.copy()
data_to_save.to_csv(r"D:\OneDrive\桌面\Cleaned CTIS data.csv", index=False, encoding='utf-8-sig')
print("Saved cleaned dataset to: D:\\OneDrive\\桌面\\Cleaned CTIS data.csv")
print()

# ================================================
# ==== Updated Statistics after Preprocessing ====
# ================================================
# Import cleaned data
cleaned_data = pd.read_csv(r"D:\OneDrive\桌面\Cleaned CTIS data.csv")

# ==== Data Distribution after Preprocessing ====
core_num_cols1 = ['estimate', 'population', 'se', 'setting_average']
cleaned_numeric_cols = list(cleaned_data.select_dtypes(include=[np.number]).columns)

# Draw the data distribution of numeric columns after Preprocessing
plt.figure(figsize=(15, 6))
for i, col in enumerate(core_num_cols1):
	plt.subplot(1, 4, i + 1)
	sns.histplot(cleaned_data[col].dropna(), kde=True)
	plt.title(f'Distribution of {col} after Preprocessing')

plt.tight_layout()
plt.show()

# The variation of data description
print("Get summary statistics for numeric columns after preprocessing:")
print(cleaned_data.describe())
print()

# Data Quality Metrics
print("Skewness Comparison (Before and After)")

original_skew = data[numeric_cols].skew()
print("Original Numerical Skew:")
print(original_skew)
print()

cleaned_skew = cleaned_data[cleaned_numeric_cols].skew()
print("Cleaned Numerical Skew:")
print(cleaned_skew)
