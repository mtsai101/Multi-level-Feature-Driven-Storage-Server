import sklearn
import pandas as pd
from influxdb import InfluxDBClient
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler

DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'storage')
max_ = 0
min_ = 0

if __name__=="__main__":
    ## Note that the input values to PCA should not be negative or the principle would be different because of the order 
    table_name = "visual_features_entropy_unnormalized"
    result = DBclient.query('SELECT * FROM '+table_name)
    result_point = pd.DataFrame(result.get_points(measurement=table_name))
    meta_X = result_point['name']

    X = result_point.drop(columns = ['name', 'time'])
    
    
    pca = PCA(n_components=1)
    y = pca.fit_transform(X)

    # minMax normalization
    max_ = max(max_, y.max(axis=0))
    min_ = min(min_, y.min(axis=0))
    y_std = (y - min_) / (max_ - min_)

    data_points = []

    for i, x in meta_X.iteritems():
        data_points.append({
            "measurement": "visual_features_entropy_PCA_normalized",
            "tags": {
                "name": str(meta_X[i])
            },
            "fields": {
                "value": float(y_std[i][0])
            }
        })
    
    DBclient.write_points(data_points, database='storage', time_precision='ms', batch_size=meta_X.size, protocol='json')
