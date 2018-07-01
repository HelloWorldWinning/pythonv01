from database import DATABASE

data_base_of_raw_data = DATABASE()
data_base_of_raw_data.database_chose("bar")
data_base_of_raw_data.collection_chose("raw_vector01")

print(data_base_of_raw_data.collection)


class VECTORS_REDUCE():

    def __init__(self, data_base_of_raw_data, n_components=200, batch_size=600,reset_pca =True):
        self.data_base = data_base_of_raw_data
        self.second = 10 * 60 * 60
        self.n_components = n_components
        self.batch_size = batch_size
        self.reset_pca = True
        if self.reset_pca is True:
            self._Set_Ipca()

    def _Moive_Name_List(self, movie_name_list):
        if isinstance(movie_name_list, str):
            movie_name_list = [movie_name_list]
        self.movie_name_list = movie_name_list
        return self.movie_name_list

    def _Set_Ipca(self, n_components=None, batch_size=None, copy=True):

        if n_components is None:
            n_components = self.n_components
        if batch_size is None:
            batch_size = self.batch_size

        from sklearn.decomposition import IncrementalPCA
        IPCA = IncrementalPCA(n_components=n_components, batch_size=batch_size, copy=copy)
        self.Ipca = IPCA
        return self.Ipca

    def _Dynamic_Chunk_Time(self, movie_name=None, chunk_min=200, chunk_max=600, max_second=100):
        """ return chunk_time second and chunk block count
        """

        if movie_name is None:
            raise ValueError(" there is no movie_name")
        data_base = self.data_base
        for chunk_time in range(8, max_second):
            left = 0
            right = chunk_time
            cursor = data_base.collection.find(
                {"movie_name": movie_name, "second": {"$gte": left, "$lt": right}},
                {"_id": True},  # "second":True,
                batch_size=500000,
                #                 cursor_type= pymongo.CursorType.EXHAUST
            )
            if cursor.count() >= chunk_min and cursor.count() <= chunk_max:
                #             print("chunk_time =",chunk_time,"cursor.count() =",cursor.count())
                self.chunk_time = chunk_time
                self.chunk_count = cursor.count()
                return self.chunk_time, self.chunk_count

        raise Exception("can not get time chunk_time second")

    #     chunk_time,chunk_count = Dynamic_Chunk_Time(movie_name=movie_name)

    def Train_Ipca(self, movie_name):

        data_base = self.data_base
        chunk_time, chunk_count = self._Dynamic_Chunk_Time(movie_name=movie_name)

        i = 0

        while True:

            left = i * chunk_time
            right = (i + 1) * chunk_time

            # what data you want
            cursor = data_base.collection.find(
                {"movie_name": movie_name, "second": {"$gte": left, "$lt": right}},
                {"second": True, "_id": False},
                batch_size=50000,
                #     cursor_type= pymongo.CursorType.EXHAUST
            )
            print("  one time chunk_time  ".center(70, "="))
            print(cursor.count())

            if cursor.count() < self.n_components:
                print("  cursor.count() < n_components  ")
                break
            if cursor.count() == 0:
                print(" cursor.count() == 0 ")
                break

            cursor_dict = list(cursor)
            dict_list = [list(one_dict.values()) for one_dict in cursor_dict]

            print(dict_list[-6:], len(dict_list))


            i += 1

            # if i % 10000 == 0 and self.reset_pca:
            #     with open('/data/bar03/ipcav05.pkl', 'wb') as file_id:
            #         pickle.dump(self.Ipca, file_id)


bar = VECTORS_REDUCE(data_base_of_raw_data)
bar.Train_Ipca(0)
