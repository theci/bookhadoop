'''
Created on 2023. 1. 23.

@author: joseph
'''
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder

from pymongo import MongoClient
import pandas as pd

class Mongo2DLFeat(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self._mongo = MongoClient('localhost', 27017)
        self._database = 'etlmongodb'
        
    def getSeries(self, coll_name):
        query = {}

        cursor = self._mongo[self._database][coll_name].find(query)
        list_cur = list(cursor)

        df = pd.DataFrame(list_cur, columns=['state', 'date', 'value'])
        # MongoDB의 시간대는 UTC로서 우리나라 시간대와 9시간이 차이납니다.
        # 로컬 시간대를 사용하기 위해 9시간을 추가합니다.
        df['date'] = df['date'] + pd.DateOffset(hours=9)
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df.set_index(['state', 'date'], inplace=True)
        collection = ('_').join(coll_name.split('_')[1:])
        df.rename(columns = {'value' : collection}, inplace=True)

        return df
    
    def getDataFrame(self):
        series_earn_Construction_month = self.getSeries('coll_earn_Construction_month')
        series_earn_Education_and_Health_Services_month = self.getSeries('coll_earn_Education_and_Health_Services_month')
        series_earn_Financial_Activities_month = self.getSeries('coll_earn_Financial_Activities_month')
        series_earn_Goods_Producing_month = self.getSeries('coll_earn_Goods_Producing_month')
        series_earn_Leisure_and_Hospitality_month = self.getSeries('coll_earn_Leisure_and_Hospitality_month')
        series_earn_Manufacturing_month = self.getSeries('coll_earn_Manufacturing_month')
        series_earn_Private_Service_Providing_month = self.getSeries('coll_earn_Private_Service_Providing_month')
        series_earn_Professional_and_Business_Services_month = self.getSeries('coll_earn_Professional_and_Business_Services_month')
        series_earn_Trade_Transportation_and_Utilities_month = self.getSeries('coll_earn_Trade_Transportation_and_Utilities_month')
        series_unempl_month = self.getSeries('coll_unempl_month')

        '''결측값 처리'''
        df = pd.concat([series_earn_Construction_month,
                        series_earn_Education_and_Health_Services_month,
                        series_earn_Financial_Activities_month,
                        series_earn_Goods_Producing_month,
                        series_earn_Leisure_and_Hospitality_month,
                        series_earn_Manufacturing_month,
                        series_earn_Private_Service_Providing_month,
                        series_earn_Professional_and_Business_Services_month,
                        series_earn_Trade_Transportation_and_Utilities_month,
                        series_unempl_month], axis=1)
        df.interpolate()
        #df.fillna(df.mean())

        ''' feature 스케일링'''
        scaler = MinMaxScaler()
        scaler.fit(df)
        ndarray_scaled = scaler.transform(df)
        df_scaled = pd.DataFrame(data=ndarray_scaled, columns=df.columns, index=df.index)
        
        return df_scaled

    def getFeature(self):
        df = self.getDataFrame()
        df.reset_index(inplace=True)

        ''' One Hot Encoding '''
        ohe = OneHotEncoder(sparse=False)
        ohe.fit(df[['state']])
        one_hot_encoded = ohe.transform(df[['state']])
        ohe_df = pd.DataFrame(one_hot_encoded, columns=ohe.categories_[0])

        df = pd.concat([df, ohe_df], axis=1)

        return df