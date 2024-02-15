from flask import Flask, render_template, jsonify
import pandas as pd
import pymysql
import numpy as np
import pymysql
import json 
from scipy.spatial import distance
from sqlalchemy import create_engine, text


def calc_inside_distance(position, white_line):
    closest_index = distance.cdist([position], white_line).argmin()
    closest_point = white_line[closest_index]
    distance_to_closest = distance.euclidean(position, closest_point)
    lap_frac = np.round(closest_index/len(white_line),4)
    return distance_to_closest, closest_index, lap_frac
pd.options.mode.chained_assignment = None 
db_url = f"mysql+mysqlconnector://23xi_read:6UsGXCWJog35s#9o@twentythree-eleven-general-db.cbywwdy08bsq.us-east-1.rds.amazonaws.com:3306/core"
engine = create_engine(db_url)

app = Flask(__name__)



# Function to fetch data from the database (replace with your actual database query)
def fetch_data_from_db():

    connection = engine.connect()
    query  = text("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY vehicle_no, CAST(lap_fraction AS SIGNED) ORDER BY last_loop_timestamp DESC) AS row_num FROM core.erdp_location_23kan2 WHERE (CAST(lap_fraction * 10000 AS SIGNED) % 10000) > 9800 AND last_loop_name = 'L7') AS subquery WHERE row_num = 1;")
    result = connection.execute(query)
    data = result.fetchall()
    k = pd.DataFrame(data)
    k['flag_change'] = k.groupby('vehicle_no').flag_code.diff()
    k['lap'] = k['lap_fraction'].astype(str).str.split('.').str[0].astype(float)
    k.flag_change.fillna(-1, inplace = True)
    k2 = k[k['flag_change'] == -1]
    # Assuming your DataFrame is named 'df' and the column you're interested in is 'column_name'
    value_counts = k2['lead_lap'].value_counts()

    # Filter values that occur more than 3 times
    filtered_values = value_counts[value_counts > 10].index.tolist()
    filtered_values.sort()

    with open(f'track_data/kan.json') as file:
        track = json.load(file)
    white_line_x = track['boundary']['inside']['x']
    white_line_y = track['boundary']['inside']['y']
    white_line = list(zip(white_line_x,white_line_y))
    df_list = []

    for i in range(1,len(filtered_values)):
        print(filtered_values[i])
        restart = k[k['lap'] == filtered_values[i]-1]
        restart['pos'] = restart.last_loop_timestamp.rank()
        pos = list(zip(restart.vehicle_position_x.astype(float), restart.vehicle_position_y.astype(float)))

        dist_white_line = list()
        for j in pos:
            dist_white_line.append(calc_inside_distance(j, white_line)[0])
        restart['dist_wl'] = dist_white_line
        restart['lane'] = restart.dist_wl.apply(lambda x: 'inside' if x < np.mean(dist_white_line) else 'outside')
        restart['restart_pos'] = restart.groupby('lane').pos.rank()
        restart[['vehicle_no','restart_pos','dist_wl', 'lane', 'lap_fraction']].sort_values(['restart_pos','lane'])

        query = text(f'SELECT * FROM core.erdp_result WHERE lead_lap = {filtered_values[i] + 3} AND race_id = 5301 AND run_type = 3')
        result = connection.execute(query)
        out_ret = result.fetchall()
        ret = pd.DataFrame(out_ret)
        if len(ret) < 10:
            query = text(f'SELECT * FROM core.erdp_result WHERE lead_lap = {filtered_values[i] + 2} AND race_id = 5301 AND run_type = 3')
            result = connection.execute(query)
            out_ret = result.fetchall()
            ret = pd.DataFrame(out_ret)

        if len(ret) > 10:
            print(f'success: {filtered_values[i]}')
            sort_out = ret.sort_values('position')[['vehicle_no', 'position']]
            r = restart[['vehicle_no','restart_pos','dist_wl', 'lane']].sort_values(['restart_pos','lane']).reset_index().reset_index()
            r['start_pos'] = r['level_0'] + 1
            r = r[['vehicle_no','restart_pos', 'lane', 'start_pos']]
            r = pd.merge(r, sort_out, on = 'vehicle_no')
            r['gain'] = r['start_pos'] - r['position']
            df1 = r.pivot_table(index='restart_pos', columns='lane', values='vehicle_no', aggfunc='first')
            df2 = r.pivot_table(index='restart_pos', columns='lane', values='position', aggfunc='first')
            df3 = r.pivot_table(index='restart_pos', columns='lane', values='gain', aggfunc='first')

            out = pd.concat([df1,df2, df3], axis = 1)
            out.index.names = ['Lane']
            out.insert(2,'V','')
            out.insert(5,'C','')

        #out.columns = ['Car # Inside','Car # Outside','','Position After Sort','','','Gain/Loss','']
            out.columns = pd.MultiIndex.from_tuples([('Car # Inside', ''), ('Car # Outside', ''),  ('', ''), ('Position After Sort', ''),('Position After Sort', ''),('', '') ,('Gain/Loss', ''), ('Gain/Loss', '')])
            df_list.append(out)
            
    print(len(df_list))
    return df_list

@app.route('/')
def display_data():
    data = fetch_data_from_db()
    return render_template('index.html', data=data)

@app.route('/get_data')
def get_data():
    data = fetch_data_from_db()
    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(debug=False)