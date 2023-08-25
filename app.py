# Import the dependencies.
import numpy as np
import datetime as dt
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with = engine)

# Save references to each table
class_measurement = Base.classes.measurement
class_station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():

    return(
        f"Available Routes: <br/>"
        f"/api/v1.0/precipitation <br/>"
        f"/api/v1.0/stations <br/>"
        f"/api/v1.0/tobs <br/>"
        f"/api/v1.0/start <br/>"
        f"---where start is of the format year-month-day <br/>"
        f"/api/v1.0/start/end <br/>"
        f"---where start and end are of the format year-month-day <br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    #find most recent date
    recent_date = session.query(class_measurement.date).order_by(class_measurement.date.desc()).first()
    #change to date type
    dt_recent_date = dt.datetime.strptime(recent_date[0], "%Y-%m-%d").date()
    #find date one year ago
    year_ago = dt_recent_date - dt.timedelta(days = 365)

    #query for past year of prcp data
    query_date_prcp = session.query(func.DATE(class_measurement.date), class_measurement.prcp).filter(func.DATE(class_measurement.date) >= year_ago)

    #create dict where date is key and prcp is value
    date_prcp_dict = {}
    for date_item, prcp_item in query_date_prcp:

        date_prcp_dict[date_item] = prcp_item

    return jsonify(date_prcp_dict)

@app.route("/api/v1.0/stations")
def stations():
    #find all stations
    list_stations = session.query(class_measurement.station).distinct().all()

    #list of stations
    list_stations = [list_item[0] for list_item in list_stations]

    return jsonify(list_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    #find all stations
    list_stations = session.query(class_measurement.station).distinct().all()

    #list of stations
    list_stations = [list_item[0] for list_item in list_stations]

    count_station_data = []
    #find station counts
    for list_item in list_stations:
    
        count_station = session.query(class_measurement).filter(class_measurement.station == list_item).count()
    
        count_station_data.append({
            "Station": list_item,
            "Count": count_station
        })
    
    count_station_df = pd.DataFrame(count_station_data).sort_values("Count", ascending = False)
    most_active_station = count_station_df.iloc[0]["Station"]
    
    #find most recent date
    recent_date = session.query(class_measurement.date).order_by(class_measurement.date.desc()).first()
    #change to date type
    dt_recent_date = dt.datetime.strptime(recent_date[0], "%Y-%m-%d").date()
    #find date one year ago
    year_ago = dt_recent_date - dt.timedelta(days = 365)

    most_active_station_data = session.query(class_measurement.date, class_measurement.tobs).filter(func.DATE(class_measurement.date) >= year_ago).filter(class_measurement.station == most_active_station).all()
    most_active_station_data = [data_item[1] for data_item in most_active_station_data]
    
    return jsonify(most_active_station_data)

@app.route("/api/v1.0/<start>")
def start_date(start):
    #remove any non-integer characters from start
    clean_start = ''.join(c for c in start if c.isdigit())
    #convert clean_start to date type and format as yyyy-mm-dd
    clean_start = dt.datetime.strptime(clean_start, '%Y%m%d').strftime('%Y-%m-%d')
    
    #https://stackoverflow.com/questions/43133605/convert-integer-yyyymmdd-to-date-format-mm-dd-yyyy-in-python

    #find min, avg, max
    temp_query = session.query(class_measurement.tobs).filter(func.DATE(class_measurement.date) >= clean_start)
    min_temp = temp_query.order_by(class_measurement.tobs.asc()).first()
    avg_temp = session.query(func.avg(class_measurement.tobs)).filter(func.DATE(class_measurement.date) >= clean_start).first()
    max_temp = temp_query.order_by(class_measurement.tobs.desc()).first()
    
    try:
        return jsonify([min_temp[0], avg_temp[0], max_temp[0]])

    except:
        return(f"Something went wrong. Try a different start date.")


@app.route("/api/v1.0/<start>/<end>")
def start__end_date(start, end):
    #remove any non-integer characters from start
    clean_start = ''.join(c for c in start if c.isdigit())
    #convert clean_start to date type and format as yyyy-mm-dd
    clean_start = dt.datetime.strptime(clean_start, '%Y%m%d').strftime('%Y-%m-%d')
    #remove any non-integer characters from end
    clean_end = ''.join(c for c in end if c.isdigit())
    #convert clean_end to date type and format as yyyy-mm-dd
    clean_end = dt.datetime.strptime(clean_end, '%Y%m%d').strftime('%Y-%m-%d')
    
    #https://stackoverflow.com/questions/43133605/convert-integer-yyyymmdd-to-date-format-mm-dd-yyyy-in-python

    #find min, avg, max
    temp_query = session.query(class_measurement.tobs).filter(func.DATE(class_measurement.date) >= clean_start).filter(func.DATE(class_measurement.date) <= clean_end)
    min_temp = temp_query.order_by(class_measurement.tobs.asc()).first()
    avg_temp = session.query(func.avg(class_measurement.tobs)).filter(func.DATE(class_measurement.date) >= clean_start).filter(func.DATE(class_measurement.date) <= clean_end).first()
    max_temp = temp_query.order_by(class_measurement.tobs.desc()).first()
    
    try:
        return jsonify([min_temp[0], avg_temp[0], max_temp[0]])
    
    except:
        return(f"Something went wrong. Try a different start and/or end date.")

if __name__ == "__main__":
    app.run(debug=True)