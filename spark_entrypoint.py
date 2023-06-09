
from pyspark.sql import SparkSession

# from app.main import main


# main()


import os

print(os.listdir("/home/hadoop/environment/"))
print(os.listdir("/home/hadoop/environment/bin/"))

import os 

# def print_tree(rootDir):
#     files = []
#     files.append(rootDir)
#     list_dirs = os.walk(rootDir) 
#     for root, dirs, files in list_dirs: 
#         for d in dirs: 
#             files.append(os.path.join(root, d))      
#         for f in files: 
#             files.append(os.path.join(root, f))
#     return files

# files = print_tree("/home/hadoop/")

# [print(f) for f in files]