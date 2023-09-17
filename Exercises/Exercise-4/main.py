import os
import json
from flatten_json import flatten
import csv

def main():
    root="data"
    for root, dirs, files in os.walk(".", topdown=False):        
        for name in files:
            root_folders=os.path.join(root, name)
            if root_folders.endswith('json'):
                with open(root_folders, 'r') as read:
                    data=json.load(read)       
                fileName=name.replace('json','csv')
                flattenJson=flatten(data)
                with open(fileName, 'w') as csvf:
                    writer=csv.DictWriter(csvf,flattenJson)
                    writer.writerow(flattenJson)
        
if __name__ == "__main__":
    main()
