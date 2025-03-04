import pandas as pd
import fundamentus as fd

# Get the data
data = fd.get_resultado_raw()

# add the update date

data['update date'] = pd.to_datetime('today').strftime('%Y-%m-%d')

# read the previous data
previous_data = pd.read_parquet('data/fundamentus_data.parquet')

# append the new data
data = pd.concat([previous_data, data])

data['ticker'] = data.index

# drop duplicates
data.drop_duplicates(subset = ['ticker','update date'], keep = 'last', inplace = True)

# drop ticker column
data.drop(columns = 'ticker', inplace = True)

# Save the data in parquet
data.to_parquet('data/fundamentus_data.parquet', engine = 'pyarrow')