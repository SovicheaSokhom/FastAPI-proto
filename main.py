from fastapi import FastAPI
import mysql.connector
from data_pb2 import Record, RecordList
from fastapi.responses import Response


app = FastAPI()
mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="Vichea123",
  database="laravel"
)

# Create a cursor
mycursor = mydb.cursor()

# Execute a select statement
mycursor.execute("SELECT * FROM songs LIMIT 5000")

# Retrieve data
result = mycursor.fetchall()


@app.get("/")
async def root():
    return {"message" : "Hello World!"}


@app.get("/data")
async def get_data():
    return {"data": result}

@app.get("/proto_data")
async def get_proto_data():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Vichea123",
        database="laravel"
    )

    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM songs LIMIT 5000")
    rows = [Record(id=row[0], name=row[1], singer=row[2]) for row in mycursor.fetchall()]

    # Close the MySQL cursor and connection
    mycursor.close()
    mydb.close()

    # Create a RecordList message and serialize it to binary format
    record_list = RecordList(Records=rows)
    serialized_data = record_list.SerializeToString()

    return Response(content=serialized_data)
