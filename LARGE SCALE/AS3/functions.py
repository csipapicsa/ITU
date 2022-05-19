# funtions
import imports as ii


# query get_data
def get_data(results):
    values = results.raw['series'][0]['values']
    columns = results.raw['series'][0]['columns']
    df = ii.pd.DataFrame(values, columns=columns).set_index('time')
    df.index = ii.pd.to_datetime(df.index)
    return df


def query(days):
    print("Query is running")
    generation_query = "SELECT  * FROM Generation where time > now() - " + str(days) +"d"
    generation = ii.client.query(
        generation_query)
    wind_query = " SELECT * FROM MetForecasts where time > now() -"+str(days)+"d and time <=now() and Lead_hours = '1' "    
    wind = ii.client.query(wind_query)
    gen_df = get_data(generation)
    # get rid of negative values 
    gen_df = gen_df[gen_df["Total"] > 0]
    wind_df = get_data(wind)
    return gen_df, wind_df
    
def clean_df(gen_df, wind_df):
    print("Dataframe cleaning is running")
    gen_df.drop(['ANM', 'Non-ANM'], axis = 1, inplace = True)
    wind_df.dropna(inplace = True)
    return gen_df, wind_df
    
    
def aggregate(df):
    return df.resample('3H').mean()
    
    
def wind_direction_transformer(df):
    print("Wind direction tranformer is running")
    dic_deg = {'N':0, 'NNE': 22.5,'NE':45, 'ENE':67.5, 
    'E':90, 'ESE':112.5, 'SE':135, 'SSE':157.5, 
    'S':180, 'SSW':202.5, 'SW':225, 'WSW':247.5, 
    'W':270, 'WNW':292.5, 'NW':315, 'NNW':337.5 }
    list_of_degrees = []
    for i in df["Direction"]:
        list_of_degrees.append(dic_deg[i])
    df['Angle'] = list_of_degrees
    return df
    
    
def data_finalizer(df):
    # drop NA values, modify columns
    final_data=df.reset_index(level=0)
    final_data.drop(columns=['time'], inplace=True)
    final_data.rename(columns={'Total':'Energy'}, inplace=True)
    final_data = final_data.dropna()
    return final_data
    
    
### pipeline
from sklearn.base import BaseEstimator, TransformerMixin

class Debugging(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self # nothing else to do
    def transform(self, X, y=None):
        self.shape = X.shape
        self.data = X
        return X
    
class KNN(BaseEstimator, TransformerMixin):
    def fit(self, X, y):
        model = KNeighborsRegressor(n_neighbors,algorithm)
        model.fit(X, y)
        return model
    def transform(self, X, y=None):
        self.shape = X.shape
        self.data = X
        return X 
        
pipe = ii.Pipeline([
    ('Debug', Debugging()),
    #('scalling', StandardScaler()),
    ('KNN', ii.KNeighborsRegressor())])


param_grid = {
    'KNN__n_neighbors': range(1,100),
    'KNN__algorithm': ["auto", "ball_tree", "kd_tree", "brute"]}
    
    
def save_predictions(df):
    dateTimeObj = ii.datetime.now()
    save_time = str(dateTimeObj.year)+'-'+str(dateTimeObj.month)+'-'+str(dateTimeObj.day)+'-'+str(dateTimeObj.hour)+'-'+str(dateTimeObj.minute)+'-'+str(dateTimeObj.second)
    df["prediction_time"] = save_time
    df.to_csv('predictions/predictions'+save_time+'.csv')
    return None
    
def time():
    dateTimeObj = ii.datetime.now()
    save_time = str(dateTimeObj.year)+'-'+str(dateTimeObj.month)+'-'+str(dateTimeObj.day)+'-'+str(dateTimeObj.hour)+'-'+str(dateTimeObj.minute)+'-'+str(dateTimeObj.second)
    return save_time
    
def load_previous_predictions():
    # to do
    return df
    
    
def load_forecast():
    # Get all future forecasts regardless of lead time
    forecasts  = ii.client.query(
    "SELECT * FROM MetForecasts where time > now()"
    ) # Query written in InfluxQL
    for_df = get_data(forecasts)

    # Limit to only the newest source time
    newest_source_time = for_df["Source_time"].max()
    newest_forecasts = for_df.loc[for_df["Source_time"] == newest_source_time].copy()

    # Preprocess the forecasts and do predictions in one fell swoop 
    # using your best pipeline.

    newest_forecasts = wind_direction_transformer(newest_forecasts)
    x_test= newest_forecasts[['Speed', 'Angle']]
    
    return newest_forecasts, x_test