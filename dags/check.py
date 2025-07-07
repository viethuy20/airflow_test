import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pyspark_file.connect_pyspark import create_dataframe

# Code DAG của bạn
print(sys.path)