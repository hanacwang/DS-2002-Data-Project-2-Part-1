import pandas as pd
import requests
import schedule
from datetime import datetime, timedelta
import sqlite3

connection =sqlite3.connect('database.db')
cursor = connection.cursor()

create_table_query = '''
    CREATE TABLE IF NOT EXISTS data_table(
        timestamp TEXT PRIMARY KEY,
        factor INT,
        pi DECIMAL
    )
'''
cursor.execute(create_table_query)

factor_list=[]
pi_list=[]
time_list=[]
minutes = 0

def get_data():
    try:
        url = "https://4feaquhyai.execute-api.us-east-1.amazonaws.com/api/pi"
        response = requests.get(url)
        data = response.json()
        factor, pi, time = data['factor'], data['pi'], data['time']

        if time in time_list:
            return

        factor_list.append(factor)
        pi_list.append(pi)
        time_list.append(time)

        cursor.execute('INSERT INTO data_table (timestamp, factor, pi) VALUES (?, ?, ?)', (time, factor, pi))
        connection.commit()

    except Exception as e:
        print(f"Error: {e}")

while True:
    if datetime.now().minute== 00:
        break

schedule.every().minute.at(":00").do(get_data)
now = datetime.now()
while minutes < 60:
    if datetime.now().second == 0 and datetime.now().minute != now.minute:
        schedule.run_pending()
        now = datetime.now()

cursor.close()
connection.close()

dataframe = pd.DataFrame({
    "timestamp":time_list,
    "factor": factor_list,
    "pi": pi_list
})

dataframe.to_csv("data.csv", index=False)