import pandas as pd
#fillin all the nan with 0
file ="<Path of combined_transactions.csv>"
df = pd.read_csv(file)
df.fillna(0, inplace=True)
df.to_csv(file, index=False)