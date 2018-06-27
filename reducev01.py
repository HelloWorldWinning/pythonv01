import pickle
import pymongo
# print("you are shit")
# from keras.preprocessing import image
# image.load_img
import pandas as pd
import numpy as np
import datetime
from pymongo import MongoClient
import pymongo
import json
import time
from database import DATABASE
# class DATABASE():
#
#     def __init__(self, bindip="127.0.0.1", port="27017"):
#         from pymongo import MongoClient
#         database_path = "mongodb://" + bindip + ":" + port + "/"
#         self.client = MongoClient(database_path)
#         self.database_names = self.client.database_names()
#         self.collections_of_eachdatabase = [
#             {database_name: [collection for collection in self.client[database_name].collection_names()]} for database_name
#             in self.client.database_names()  if database_name not in ["admin","config","local"] ]
#
#     def database_chose(self, database_name):
#         self.database = self.client[database_name]
#
#     def collection_chose(self, collection_name):
#         self.collection = self.database[collection_name]
#
#     def get_data(self, movie_name_field="movie_name", movie_name=None, limit = None,numpy_data=True, *args, **kwargs):
#         if movie_name is None:
#             query_str = {"$lte": 10000}
#         else:
#             query_str = movie_name
#
#         if limit == None:
#             cursor = self.collection.find({movie_name_field: query_str}, {"_id": False})
#         else:
#             cursor = self.collection.find({movie_name_field: query_str}, {"_id": False}).limit(limit)
#         data_list_from_db = list(map(lambda x: list(x.values()), cursor))
#         if numpy_data:
#             return np.array(data_list_from_db)
#         else:
#             return data_list_from_db
#
#     def insert_data(self, d2_arrary_data, d2_target):
#         dimension_of_data = d2_arrary_data.shape[1]
#         df_data = pd.DataFrame(data=d2_arrary_data, columns=range(dimension_of_data))
#         df_target = pd.DataFrame(data=d2_target, columns=["movie_name", "second"])
#         data_target = df_data.join(df_target)
#         insert_result = self.collection.insert_many(json.loads(data_target.to_json(orient="records")))
#         return insert_result

# from sklearn.decomposition import PCA, IncrementalPCA

data_base = DATABASE()
data_base.collections_of_eachdatabase

data_base.database_chose("bar")
data_base.collection_chose("raw_vector01")
print("data_base.collection =", data_base.collection)


def Explain_Ratio(numpy_array, percentage=0.95, first=50):
    numpy_array = np.array(numpy_array).flatten()
    sum_ = 0
    for index in range(len(numpy_array)):
        sum_ += numpy_array[index]
        if sum_ >= percentage:
            return index + 1, (index + 1) / len(numpy_array), len(numpy_array), numpy_array[:first].sum(), numpy_array[
                                                                                                           :2 * first].sum()

    return numpy_array.sum(), numpy_array[:first].sum(), numpy_array[:2 * first].sum()


data_base_redu =DATABASE()
data_base_redu.database_chose("bar")
data_base_redu.collection_chose("raw_vector01_redu")
print("data_base_redu.collection",data_base_redu.collection)

from sklearn.decomposition import IncrementalPCA
#
# IPCA = IncrementalPCA(n_components=50, batch_size=1500)
#
# hour = 1
# mimute = 5
# second = hour * mimute * 60
#
# cursor = data_base.collection.find(
#     {"movie_name": 3, "second": {"$lte": second}},
#     {"second": True, "_id": False},
#     batch_size=5000,
#     cursor_type=pymongo.CursorType.EXHAUST
# )
# # cursor_dict = list(cursor)
# # dict_list  =[ list(one_dict.values()) for one_dict in cursor_dict  ]
# # dict_list
# # dict_list
#
# chunk = 100
# length = cursor.count()
# print("  cursor.count()  = {}  ".format(length).center(80, "="))
# # print("cursor.count() = ",length )
#
# i = 0
# while i * chunk < length:
#     # print("=" * 30)
#     start_of_slice = i * chunk
#     end_of_slice = min((i + 1) * chunk, length)
#
#     if (length - (i + 1) * chunk) <= 0:
#         temp_chunk = length - i * chunk
#     else:
#         temp_chunk = chunk
#
#     #     for index, element in enumerate(dict_list[start_of_slice:end_of_slice]):
#     #         print("chunk =",i,index,element)
#
#     #    { "movie_name":True,"second":True, "_id":False}  ,
#
#     cursor = data_base.collection.find(
#         {"movie_name": 3, "second": {"$lte": second}},
#         {"_id": False},
#         batch_size=5000,
#         cursor_type=pymongo.CursorType.EXHAUST
#     )[start_of_slice:end_of_slice]
#
#     cursor_dict = list(cursor)
#     data_dict_list = [list(one_dict.values())[:-2] for one_dict in cursor_dict]
#     target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]
#
#     #     print(dict_list)
#
#     IPCA.partial_fit(data_dict_list)
#     print(" new chunk  ".center(60, "#"))
#     print("chunk = ", i, "first50", IPCA.explained_variance_ratio_[:50].sum(),
#           IPCA.explained_variance_ratio_[:10])
#     print("Explain_Ratio(IPCA.explained_variance_ratio_", Explain_Ratio(IPCA.explained_variance_ratio_))
#     #     cursor = data_base.collection.find(
#     #     {"movie_name":3,"second":{"$lte":2}} ,
#     #     { "movie_name":False, "_id":False} ,
#     #     batch_size =5000,
#     # #     cursor_type= pymongo.CursorType.EXHAUST
#     #      )
#
#     i += 1
#
# # save the classifier
# with open('/data/bar03/ipcav02.pkl', 'wb') as file_id:
#     pickle.dump(IPCA, file_id)
#
# test_data = np.random.rand(10, 204800)
#
# test_data_ipcad = IPCA.transform(test_data)
#
# # load it again
# with open('/data/bar03/ipcav02.pkl', 'rb') as file_id:
#     IPCA_loaded = pickle.load(file_id)
#
# test_data_ipcad_loaded = IPCA.transform(test_data)
#
# print(   (test_data_ipcad_loaded == test_data_ipcad).all() )
#
#



#####################################

print("   ALL    ".center(100, "$"))
all_result = []
from sklearn.decomposition import IncrementalPCA

IPCA = IncrementalPCA(n_components=200, batch_size=500)

hour = 1
mimute = 5
second = 2 * 60 * 60

cursor = data_base.collection.find(
    {"movie_name": {"$lte": 9}, "second": {"$lte": second}},
    {"second": True, "_id": False},
    batch_size=5000,
    cursor_type=pymongo.CursorType.EXHAUST
)
# cursor_dict = list(cursor)
# dict_list  =[ list(one_dict.values()) for one_dict in cursor_dict  ]
# dict_list
# dict_list

chunk = 300
length = cursor.count()
print("  cursor.count()  = {}  ".format(length).center(80, "="))
# print("cursor.count() = ",length )

i = 0
while i * chunk < length:
    # print("=" * 30)
    start_of_slice = i * chunk
    end_of_slice = min((i + 1) * chunk, length)

    if (length - (i + 1) * chunk) <= 0:
        temp_chunk = length - i * chunk
    else:
        temp_chunk = chunk

    cursor = data_base.collection.find(
        {"movie_name": {"$lte": 9}, "second": {"$lte": second}},
        {"_id": False},
        batch_size=500,
        cursor_type=pymongo.CursorType.EXHAUST
    )[start_of_slice:end_of_slice]

    cursor_dict = list(cursor)
    data_dict_list = [list(one_dict.values())[:-2] for one_dict in cursor_dict]
    target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]

    print("    timing    ".center(50,"="))
    t0 =time.time()
    IPCA.partial_fit(data_dict_list)
    print("elapsed time = {}".format(time.time()-t0).center(50,"="))
    print(" new chunk  ".center(60, "#"))
    print("chunk = ", i, "first50", IPCA.explained_variance_ratio_[:50].sum(),
          IPCA.explained_variance_ratio_[:10])
    print("Explain_Ratio(IPCA.explained_variance_ratio_", Explain_Ratio(IPCA.explained_variance_ratio_))
    all_result.append(IPCA.explained_variance_ratio_)
    #     cursor = data_base.collection.find(
    #     {"movie_name":3,"second":{"$lte":2}} ,
    #     { "movie_name":False, "_id":False} ,
    #     batch_size =5000,
    # #     cursor_type= pymongo.CursorType.EXHAUST
    #      )

    i += 1

# save the classifier
with open('/data/bar03/ipcav02.pkl', 'wb') as file_id:
    pickle.dump(IPCA, file_id)

test_data = np.random.rand(10, 204800)

test_data_ipcad = IPCA.transform(test_data)

# load it again
with open('/data/bar03/ipcav02.pkl', 'rb') as file_id:
    IPCA_loaded = pickle.load(file_id)

test_data_ipcad_loaded = IPCA.transform(test_data)

print( (test_data_ipcad_loaded == test_data_ipcad).all() )
