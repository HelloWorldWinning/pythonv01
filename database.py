# print("you are shit")
# from keras.preprocessing import image
# image.load_img
import pandas as pd
import numpy as np
import datetime
from pymongo import MongoClient
import json

class DATABASE():

    def __init__(self, bindip="127.0.0.1", port="27017"):
        from pymongo import MongoClient
        database_path = "mongodb://" + bindip + ":" + port + "/"
        self.client = MongoClient(database_path)
        self.database_names = self.client.database_names()
        self.collections_of_eachdatabase = [
            {database_name: [collection for collection in self.client[database_name].collection_names()]} for database_name
            in self.client.database_names()]

    def database_chose(self, database_name):
        self.database = self.client[database_name]

    def collection_chose(self, collection_name):
        self.collection = self.database[collection_name]

    def get_data(self, movie_name_field="movie_name", movie_name=None, numpy_data=True, *args, **kwargs):
        if movie_name is None:
            query_str = {"$exists": True}
        else:
            query_str = movie_name
        cursor = self.collection.find({movie_name_field: query_str}, {"_id": False})
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
        self.collection.insert(json.loads(data_target.to_json(orient="records")))


if __name__ == "__main__":
    from pprint import pprint

    co = DATABASE()
    for i in co.collections_of_eachdatabase:
        pprint(i)

    # co.database_chose("bar2")
    # co.collection_chose("raw_vector01")
    # print("co.get_data().shape =",co.get_data().shape)
    # # print(  co.database.drop_collection())
    #
    # co = DATABASE()
    # for i in co.collections_of_eachdatabase:
    #     pprint(i)

    # co.client.drop_database("bar")
    # print("  clean  ".center(50,"="))
    # co = DATABASE()
    # for i in co.collections_of_eachdatabase:
    #     pprint(i)
