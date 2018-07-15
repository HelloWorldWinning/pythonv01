# print("you are shit")
# from keras.preprocessing import image
# image.load_img
import pandas as pd
import numpy as np
import datetime
from pymongo import MongoClient
import json
import time
class DATABASE():

    def __init__(self, bindip="127.0.0.1", port="27017"):
        from pymongo import MongoClient
        database_path = "mongodb://" + bindip + ":" + port + "/"
        self.client = MongoClient(database_path)
        self.database_names = self.client.database_names()
        self.collections_of_eachdatabase = [
            {database_name: [collection for collection in self.client[database_name].collection_names()]} for database_name
            in self.client.database_names()  if database_name not in ["admin","config","local"] ]

    def database_chose(self, database_name):
        self.database = self.client[database_name]

    def collection_chose(self, collection_name):
        self.collection = self.database[collection_name]

    def get_data(self, movie_name_field="movie_name", movie_name=None, limit = None,numpy_data=True, *args, **kwargs):
        if movie_name is None:
            query_str = {"$lte": 1000000}
        else:
            query_str = movie_name

        if limit is None:
            cursor = self.collection.find({movie_name_field: query_str}, {"_id": False})
        else:
            cursor = self.collection.find({movie_name_field: query_str}, {"_id": False}).limit(limit)
        data_list_from_db = list(map(lambda x: list(x.values()), cursor))
        if numpy_data:
            return np.array(data_list_from_db)
        else:
            return data_list_from_db

    def insert_data(self, d2_arrary_data, d2_target):
        dimension_of_data = d2_arrary_data.shape[1]
        df_data = pd.DataFrame(data=d2_arrary_data, columns=range(dimension_of_data))
        df_target = pd.DataFrame(data=d2_target, columns=["movie_name", "second"])
        data_target = df_data.join(df_target)
        insert_result = self.collection.insert_many(json.loads(data_target.to_json(orient="records")))
        return insert_result

if __name__ == "__main__":

    from pprint import pprint

    data_base = DATABASE()

    print("  old version  ".center(100,"="))
    for i in data_base.collections_of_eachdatabase:
        pprint(i)

    # data_base.database_chose("bar")
    # data_base.collection_chose("raw_vector01")


    data_base.database_chose("bar")
    data_base.collection_chose("raw_vector01_redu")


    # print(data_base.collection.find().count())
    # print(data_base.get_data( limit=10))

    # co.database_chose("bar2")
    # print(co.database.drop_collection("bar2_01"))
    # co.collection_chose("raw_vector01")
    # print( co.collection.drop() )

    # big_data =int( 1e0)
    # t0 = time.time()
    # target =  np.random.rand(big_data,2)
    # target[:,0] = 1
    # insert_result = data_base.insert_data(np.random.rand(big_data,8),target)
    # print(  " insert time cost is    = ", time.time()-t0)



    # print(insert_result)
    # print(insert_result.inserted_ids)
    # print("co.get_data().shape =",co.get_data().shape)
    # print( co.database.drop_collection("raw_vector01"))
    # co = DATABASE()
    # for i in co.collections_of_eachdatabase:
    #     pprint(i)


    # print("  new   ".center(100,"$"))


    # # co.client.drop_database("bar")
    # # print("  clean  ".center(50,"="))
    # # co = DATABASE()
    #
    # for i in co.collections_of_eachdatabase:
    #     pprint(i)
    # #
    # print(co.collection.count(),co.get_data().shape )
    # print(data_base.get_data().shape)
    # # print(data_base.get_data())
    #
    # print("  fetch data ".center(100,"!"))
    # cursor =  data_base.collection.find({ "movie_name": 1}, {"_id": False} ).limit(5)
    # data_list_from_db = np.array(  list(map(lambda x: list(x.values()), cursor)) )
    # print(data_list_from_db.shape)

    from sklearn.decomposition import IncrementalPCA as IPCA

    print( "  getting data ".center(100,"!"))
    d = data_base

    # d = data_base.collection.find({"movie_name": "$exists" } )
    # d = data_base.collection.find({"movie_name":  0 }  , {  "movie_name":True ,"_id": False}).limit(10)
    # # print( [ i for i in d])
    # print(d.count())
    # out = [i for i in d.collection.find({"movie_name": {"$exists": True}}, {"movie_name":, "_id": False})]
    # out = [i for i in d.collection.find({}, {"movie_name":{"$slice":[-20,1]}, "movie_name":True , "_id": False})]
    # out = [i for i in d.collection.find({}, {"movie_name":True, "_id": False})[30000:30000+5]]
    out =d.get_data(movie_name=0,limit=2)
    print("are you ok")
    # print(len(out))
    print(out)
