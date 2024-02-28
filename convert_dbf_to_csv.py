from simpledbf import Dbf5
import os


input = r"C:\Users\Shark\Desktop\Laisku_siuntimas\Data\\"
output = r"C:\Users\Shark\Desktop\Laisku_siuntimas\Renamed\\"

for file_name in os.listdir(input):
    source = input + file_name
    destination = output + file_name[:-3] + 'csv'
    dbf = Dbf5(str(source), codec = 'cp775')
    df = dbf.to_dataframe()
    df.to_csv(destination,encoding='utf-8')
    print(destination)