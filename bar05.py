import time
from database import DATABASE
import pymongo

data_base_of_raw_data = DATABASE()
data_base_of_raw_data.database_chose("bar")
data_base_of_raw_data.collection_chose("raw_vector01")
print(data_base_of_raw_data.collections_of_eachdatabase)
data_base = data_base_of_raw_data

page_size = 1000
movie_name = 3
cursor = data_base.collection.find({"movie_name": movie_name,'second': {'$gt': 25.1}}
                                       ).limit(page_size)
t0 = time.time()
cursor_dict = list(cursor)

length_of_chunk = cursor.count()
cursor.close()

print("len(cursor_dict)) =", length_of_chunk)
print("   read data time = {}  ".format(time.time() - t0).center(60, "*"))


