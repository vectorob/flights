import pandas as pd
import numpy as np
import pymysql
import json 
from scipy.spatial import distance

mydb = pymysql.connect(
    host="twentythree-eleven-general-db.cbywwdy08bsq.us-east-1.rds.amazonaws.com",
    port=3306,
    user="23xi_read",
    password="6UsGXCWJog35s#9o"
)

def calc_inside_distance(position, white_line):
    closest_index = distance.cdist([position], white_line).argmin()
    closest_point = white_line[closest_index]
    distance_to_closest = distance.euclidean(position, closest_point)
    lap_frac = np.round(closest_index/len(white_line),4)
    return distance_to_closest, closest_index, lap_frac
pd.options.mode.chained_assignment = None 

def main():
    mydb = pymysql.connect(
        host="twentythree-eleven-general-db.cbywwdy08bsq.us-east-1.rds.amazonaws.com",
        port=3306,
        user="23xi_read",
        password="6UsGXCWJog35s#9o"
    )
    query  = "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY vehicle_no, CAST(lap_fraction AS SIGNED) ORDER BY last_loop_timestamp DESC) AS row_num FROM core.erdp_location_curr WHERE (CAST(lap_fraction * 10000 AS SIGNED) % 10000) > 9800 AND last_loop_name = 'L7') AS subquery WHERE row_num = 1;"

    k = pd.read_sql(query, mydb)

    k['flag_change'] = k.groupby('vehicle_no').flag_code.diff()
    k['lap'] = k['lap_fraction'].astype(str).str.split('.').str[0].astype(float)
    k.flag_change.fillna(-1, inplace = True)
    k2 = k[k['flag_change'] == -1]
    # Assuming your DataFrame is named 'df' and the column you're interested in is 'column_name'
    value_counts = k2['lead_lap'].value_counts()

    # Filter values that occur more than 3 times
    filtered_values = value_counts[value_counts > 10].index.tolist()
    filtered_values.sort()

    with open(f'track_data/Texas.json') as file:
        data = json.load(file)
    white_line_x = data['boundary']['inside']['x']
    white_line_y = data['boundary']['inside']['y']
    white_line = list(zip(white_line_x,white_line_y))
    output_csv = "output_tex.csv"
    with open(output_csv, "w") as file:
        df_list = []
        for i in range(1,len(filtered_values)):
            print(i)
            restart = k[k['lap'] == filtered_values[i]-1]
            restart['pos'] = restart.last_loop_timestamp.rank()
            pos = list(zip(restart.vehicle_position_x, restart.vehicle_position_y))

            dist_white_line = list()
            for j in pos:
                dist_white_line.append(calc_inside_distance(j, white_line)[0])
            restart['dist_wl'] = dist_white_line
            restart['lane'] = restart.dist_wl.apply(lambda x: 'inside' if x < np.mean(dist_white_line) else 'outside')
            restart['restart_pos'] = restart.groupby('lane').pos.rank()
            restart[['vehicle_no','restart_pos','dist_wl', 'lane', 'lap_fraction']].sort_values(['restart_pos','lane'])

            query = f'SELECT * FROM core.erdp_result WHERE lead_lap = {filtered_values[i] + 3} AND race_id = 5303 AND run_type = 3'
            ret = pd.read_sql(query, mydb)
            if len(ret) < 10:
                query = f'SELECT * FROM core.erdp_result WHERE lead_lap = {filtered_values[i] + 2} AND race_id = 5303 AND run_type = 3'
                ret = pd.read_sql(query, mydb)
            if len(ret) > 10:
                print(filtered_values[i])
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
                file.write(f"Restart {i}\n")
                file.write(f'Lap #: {filtered_values[i]+1}\n')
                file.write(f'Sort Lap: {filtered_values[i]+3}\n')
                out.to_csv(file, index=False)
                file.write("\n")
    
if __name__ == "__main__":
    main()