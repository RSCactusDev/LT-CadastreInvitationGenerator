import pandas as pd
from sqlalchemy import create_engine
import time
import os 


start_time_total = time.time()

user = 'LTsklypai'
password = 'LTsklypai1'

engine = create_engine('mysql://' + user + ':' + password + '@localhost:3306/ltsklypai?charset=utf8')

input = r"C:\Users\Shark\Desktop\Laisku_siuntimas\Edit\\"

for file_name in os.listdir(input):
    start_time = time.time()
    source = input + file_name
    df = pd.read_csv(source, low_memory=False, encoding='utf8')
    df.to_sql(file_name[:-4],con=engine,index=False,if_exists='replace')
    print(f"Created {file_name[:-4]} table. Process finished --- {(time.time() - start_time)} seconds ---") 


print(f"Process finished --- {(time.time() - start_time_total)} seconds ---")