# print("you are shit")
# from keras.preprocessing import image
# image.load_img

import datetime
from pymongo import MongoClient


class DataBase():

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

    def get_data(self, movie_name_field="movies", movie_name=None, numpy_data=True, *args, **kwargs):
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


data_to_database = DataBase()
print(data_to_database.collections_of_eachdatabase)
data_to_database.database_chose("bar")
data_to_database.collection_chose("runoob444444")

def xxoo():
    print("xx00")

xxoo()