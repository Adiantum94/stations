from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
import csv

engine = create_engine('sqlite:///station_database.db', echo=True)


meta = MetaData()

clean_station = Table(
   'clean_station', meta,
   Column('station', String, primary_key=True),
   Column('latitude', String),
   Column('longitude', String),
   Column('elevation', String),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

clean_measure = Table(
   'clean_measure', meta,
   Column('id', Integer,  primary_key=True),
   Column('station', String, ForeignKey('clean_station.station'), nullable=False),
   Column('date', String,),
   Column('precip', String),
   Column('tobs', String),
)

meta.create_all(engine)


ins = clean_station.insert()
inss = clean_measure.insert()
conn = engine.connect()

with open('clean_stations.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    conn.execute(
        ins, 
        [{"station": row[0], "latitude": row[1], "longitude": row[2], "elevation": row[3], "name": row[4], "country": row[5], "state": row[6]} 
            for row in csv_reader]
    )



with open('clean_measure.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    conn.execute(
        inss, 
        [{"station": row[0], "date": row[1], "precip": row[2], "tobs": row[3]} 
            for row in csv_reader]
    )
