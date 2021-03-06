import sklearn
import pandas as pd
from influxdb import InfluxDBClient
from sklearn import manifold
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
from metrics.pyDRMetrics import DRMetrics
import matplotlib.pyplot as plt
import yaml

with open('/home/min/Multi-level-Feature-Driven-Storage-Server/configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient(host=data['global']['database_ip'], port=data['global']['database_port'], database=data['global']['database_name'], username='root', password='root')

max_ = 0
min_ = 0

if __name__=="__main__":
    # Note that the input values to PCA should not be negative or the principle would be different because of the order 
    table_name = "visual_features_entropy_unnormalized"
    result = DBclient.query('SELECT * FROM '+table_name)
    result_point = pd.DataFrame(result.get_points(measurement=table_name))
    meta_X = result_point['name']

    X = result_point.drop(columns = ['name', 'time'])
    
    
    isomap = manifold.Isomap(n_neighbors=12, n_components=1)
    isomap_y = isomap.fit_transform(X)

    pca = PCA(n_components=1)
    pca_y = pca.fit_transform(X)

    drm_isomap = DRMetrics(X, isomap_y)
    drm_pca = DRMetrics(X, pca_y)


    # # minMax normalization
    # max_ = max(max_, y.max(axis=0))
    # min_ = min(min_, y.min(axis=0))
    # y_std = (y - min_) / (max_ - min_)

    plt.plot(drm_pca.T, label = 'pca', color = 'b')
    plt.plot(drm_isomap.T, label = 'isomap', color = 'y')
    drm_pca.plot_distance_matrix()
    plt.ylim(-0.1,1.1)
    plt.legend()
    plt.savefig('T.png')
    plt.clf()

    plt.plot(drm_pca.C, label = 'pca', color = 'b')
    plt.plot(drm_isomap.C, label = 'isomap', color = 'y')

    plt.ylim(-0.1,1.1)
    plt.legend()
    plt.savefig('C.png')
    plt.clf()

    print('PCA T AUC = ', drm_pca.AUC_T)
    print('PCA C AUC = ', drm_pca.AUC_C)
    print('iso T AUC = ', drm_isomap.AUC_T)
    print('iso C AUC = ', drm_isomap.AUC_C)

