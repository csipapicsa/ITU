# import DF
import pandas as pd
import numpy as np
from influxdb import InfluxDBClient

# preproc, develop
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV

# math
import math
from sklearn.model_selection import cross_val_score
from sklearn.metrics import r2_score, mean_absolute_error

# Regression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder

from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# graph
import seaborn as sbn
import matplotlib.pyplot as plt

# develop
from importlib import reload
import pickle
# time
from datetime import datetime

#############################
# LOG IN
client = InfluxDBClient(host='influxus.itu.dk', port = 8086, username='lsda', password = 'icanonlyread')
client.switch_database('orkney')