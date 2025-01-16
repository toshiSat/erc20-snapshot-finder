import pandas as pd
import numpy as np

# Load the CSV file
df = pd.read_csv("nft_locks.csv")

# Convert the second column to numeric, handling non-numeric values safely
df.iloc[:, 1] = pd.to_numeric(df.iloc[:, 1], errors='coerce')

# Convert from Wei to Ether (1 Ether = 10**18 Wei)
df.iloc[:, 1] = df.iloc[:, 1] / 10**18

# Set display options to prevent scientific notation
pd.set_option('display.float_format', lambda x: '%.18f' % x)
np.set_printoptions(suppress=True, formatter={'float_kind':'{:f}'.format})

# Save the modified data back to a new CSV file
df.to_csv("output.csv", index=False, float_format='%.18f')

print("Conversion complete! The new CSV has been saved as 'output.csv'.")
