# Installing and importing packages
import pyspark
import pandas as pd
import numpy as np

# Importing dataset
data = pd.read_csv('charts.csv')
print(data.shape)