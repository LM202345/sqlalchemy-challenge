import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect


from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///./Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
measurement = Base.classes.measurement


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation"
        f"/api/v1.0/stations"
        f"/api/v1.0/tobs"
        f"/api/v1.0/<start2>"
        f"/api/v1.0/1/<start2>"
        f"/api/v1.0/<start>/<end>"

    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Convert the query results from your precipitation analysis (i.e. retrieve only the last 12 months of data) to a dictionary using date as the key and prcp as the value.
    
    ## query from precipitation analysis

    # Calculate the date one year from the last date in data set.
    recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()

    one_year_ago = dt.datetime.strptime(recent_date[0], '%Y-%m-%d') - dt.timedelta(days=366)
    print(one_year_ago)

    # Perform a query to retrieve the data and precipitation scores

    Data = session.query(measurement.date, measurement.prcp).\
        filter(measurement.date >= one_year_ago ).\
        order_by(measurement.date).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    
    precipitation = pd.DataFrame(Data, columns=['Date','Inches'])
   
    precipitation1 = precipitation.dropna(how='any')

    # Sort the dataframe by date
    precipitation_sorted = precipitation1.sort_values(by='Date')
   
 
    session.close()

    # Create a dictionary 

    precipitation_dictionary = []
    for index, row in precipitation_sorted.iterrows():
        precipitation_dict = {}
        precipitation_dict["Date"] = row['Date']
        precipitation_dict["Inches"] = row['Inches']
        precipitation_dictionary.append(precipitation_dict)

    #Return the JSON representation of your dictionary.
    return jsonify(precipitation_dictionary) 
   
  


@app.route("/api/v1.0/stations")
def station():

    # Return a JSON list of stations from the dataset.
    
    """Return a list of all stations"""
    
    hawaii_stations_path = "Resources/hawaii_stations.csv"

    # Read files
    
    hawaii_stations = pd.read_csv(hawaii_stations_path)

    # Display the data table for preview 

    station_list = pd.DataFrame(hawaii_stations, columns=['station','name'])
    
    # Convert DataFrame rows to JSON using a for loop

    station_json = []
    for index, row in station_list.iterrows():
        json_data = {
            'station': row['station'],
            'name': row['name']
        }
        station_json.append(json_data)
  
    return (station_json) 


@app.route("/api/v1.0/tobs")
def temperature():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query the dates and temperature observations of the most-active station for the previous year of data.

    # Calculate the date one year from the last date in data set.
    
    recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()

    one_year_ago = dt.datetime.strptime(recent_date[0], '%Y-%m-%d') - dt.timedelta(days=366)
    
    
    active_stations = session.query(measurement.station, func.count(measurement.station)).\
        group_by(measurement.station).\
        order_by(func.count(measurement.station).desc()).all()


    data2 = session.query(measurement.tobs).\
        filter(measurement.station == active_stations[0].station).\
        filter(measurement.date >= one_year_ago )

    session.close()


    #Return a JSON list of temperature observations for the previous year

    tobs_json = []
    for row in data2:
        json_data = {
            'tobs': row['tobs']
        }
        tobs_json.append(json_data)
  
    return (tobs_json) 


@app.route("/api/v1.0/<start>")
def average(start):

    # Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range.

    hawaii_measurements_path = "Resources/hawaii_measurements.csv"

    # Read files    
    hawaii_measurements = pd.read_csv(hawaii_measurements_path)

    # Finds specific date in the dataset 
    temp_start = pd.DataFrame(hawaii_measurements, columns=['date', 'tobs']).query("date == @start")
    if (not temp_start.empty):
        summary_stats = temp_start.agg({'tobs': ['min', 'mean', 'max']})
        return summary_stats.to_json()
    return jsonify({"error": f"Date {start} not found."}), 404



    


@app.route("/api/v1.0/1/<start2>")
def average2(start2):

    #For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.

    hawaii_measurements_path = "Resources/hawaii_measurements.csv"

    # Read files    
    hawaii_measurements = pd.read_csv(hawaii_measurements_path)

    # Finds specific date in the dataset 

    temp_start2 = pd.DataFrame(hawaii_measurements, columns=['date', 'tobs']).query("date >= @start2")
    summary_stats2 = temp_start2.agg({'tobs': ['min', 'mean', 'max']})
    
    return summary_stats2.to_json()


@app.route("/api/v1.0/<start>/<end>")
def start_end(start,end):


    #For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.

    hawaii_measurements_path = "Resources/hawaii_measurements.csv"

    # Read files    
    hawaii_measurements = pd.read_csv(hawaii_measurements_path)

    # Finds data for specific range between start date and end date in the dataset 
  
    temp_start = pd.DataFrame(hawaii_measurements, columns=['date', 'tobs']).query("date > @start and date <= @end")

    # return temp_start.to_json()

    if (not temp_start.empty):
        summary_stats = temp_start.agg({'tobs': ['min', 'mean', 'max']})
        return summary_stats.to_json()
    return jsonify({"error": f"Date {start, end} not found."}), 404
    

if __name__ == '__main__':
    app.run(debug=True)