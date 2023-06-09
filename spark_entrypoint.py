
from pyspark.sql import SparkSession

# from app.main import main


# main()


import os

print(os.listdir())

import os 
files = []
def print_tree(rootDir):
    files.append(rootDir)
    list_dirs = os.walk(rootDir) 
    for root, dirs, files in list_dirs: 
        for d in dirs: 
            files.append(os.path.join(root, d))      
        for f in files: 
            files.append(os.path.join(root, f))

print_tree("/home/hadoop/")

[print(f) for f in files]