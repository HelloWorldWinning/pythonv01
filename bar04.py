import time

from database import DATABASE

data_base_of_raw_data = DATABASE()
data_base_of_raw_data.database_chose("bar")
data_base_of_raw_data.collection_chose("raw_vector01")
data_base_of_raw_data.collections_of_eachdatabase

data_base = data_base_of_raw_data






####

def idlimit(movie_name, page_size, last_id=None):
    """Function returns `page_size` number of documents after last_id
    and the new last_id.
    """
    # if last_id is None:
    #     # When it is first page
    #     cursor = db['students'].find().limit(page_size)
    # else:
    #     cursor = db['students'].find({'_id': {'$gt': last_id}}).limit(page_size)

    if last_id is None:
        # When it is first page
        cursor = data_base.collection.find({"movie_name": movie_name}
                                           ).limit(page_size)
    else:
        cursor = data_base.collection.find({"movie_name": movie_name, '_id': {'$gt': last_id}}

                                           ).limit(page_size)
    t0 = time.time()
    cursor_dict = list(cursor)
    # dict_list = [list(one_dict.values()) for one_dict in cursor_dict]

    print("   read data time = {} ".format(time.time() - t0).center(60, "*"))

    if len(cursor_dict) < 150:
        return None,None,None


    dict_list = [list(one_dict.values())[1:-2] for one_dict in cursor_dict]
    target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]
    print("len(dict_list) =", len(dict_list),
          "len(dict_list[-1] =", len(dict_list[-1]),
          "target_list[-3:] =", target_list[-3:]
          )
    last_id = cursor_dict[-1]['_id']
    print("last_id =", last_id)

    return dict_list, target_list, last_id




    # Get the data
    # data = [x for x in cursor]
    #
    # if not data:
    #     # No documents left
    #     return None, None
    #
    # # Since documents are naturally ordered with _id, last document will
    # # have max id.
    # last_id = data[-1]['_id']
    #
    # # Return data and last_id
    # return data, last_id



####












class self():
    pass

self.n_components = 150
page_size = 187
movie_name = 0
last_id = None
i = 0

while True:
    print("  once chunk_time  ".center(90, "="))

    #     left = i * chunk_time
    #     right = (i + 1) * chunk_time

    # todo 1  what data you want

    #     cursor = data_base.collection.find(
    #         {"movie_name": movie_name, "second": {"$gte": left, "$lt": right}},
    #         {"_id": False},  # "second": True,
    #         batch_size=50000,
    #         #     cursor_type= pymongo.CursorType.EXHAUST
    #     )

    # cursor_type= pymongo.CursorType.EXHAUST
    if last_id is None:
        # When it is first page
        cursor = data_base.collection.find({"movie_name": movie_name}
                                           ).limit(page_size)
    else:
        cursor = data_base.collection.find({"movie_name": movie_name, '_id': {'$gt': last_id}}

                                           ).limit(page_size)

    # t0= time.time()
    # print(cursor.count())
    # print("cursor.count() time =",time.time()-t0)
    # = pymongo.CursorType.EXHAUST
    #     if cursor.count() < self.n_components:
    #         print("  cursor.count() < n_components  ")
    #         break
    #     if cursor.count() == 0:
    #         print(" cursor.count() == 0 ")
    #         break

    t0 = time.time()
    cursor_dict = list(cursor)
    # dict_list = [list(one_dict.values()) for one_dict in cursor_dict]

    print("   read data time = {} ".format(time.time() - t0).center(60, "*"))

    if len(cursor_dict) < self.n_components:
        break

    dict_list = [list(one_dict.values())[1:-2] for one_dict in cursor_dict]
    target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]
    print("len(dict_list) =", len(dict_list),
          "len(dict_list[-1] =", len(dict_list[-1]),
          "target_list[-3:] =", target_list[-3:]
          )
    last_id = cursor_dict[-1]['_id']
    print("last_id =", last_id)
    # i+=1
    # if i>20:
