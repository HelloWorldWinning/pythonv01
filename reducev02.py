import pickle
import subprocess

from database import DATABASE




class VECTORS_REDUCE():

    def __init__(self, data_base_of_raw_data, folder_containing_movies=None,
                 movie_name_list=None,
                 n_components=200, batch_size=600, ipca_model_path=None, reset_pca=True):
        self.data_base = data_base_of_raw_data
        self.second = 10 * 60 * 60
        self.n_components = n_components
        self.batch_size = batch_size

        if reset_pca is True:
            self._Set_Ipca()
            self.reset_pca = True
        else:
            self.reset_pca = False

        if folder_containing_movies is not None:
            self._movie_folder = folder_containing_movies
            self.movie_name_list = self._Get_Moive_Name_From_Folder()
        elif movie_name_list is not None:
            if not isinstance(movie_name_list,list):
                raise ValueError(
                    " movie_name_list must be a list object"
                )
            self.movie_name_list = movie_name_list
        else:
            raise Exception(
                "You should specify a folder that contains movie files. Or a list that contains a movie names")

        if ipca_model_path is None:
            self._model_path = "/data/bar03/ipcav05.pkl"
        else:
            self._model_path = ipca_model_path


    def _Get_Moive_Name_From_Folder(self, movie_folder=None):
        if movie_folder is None:
            movie_folder = self._movie_folder

        movie_suffix = (".3gp", ".mp4", ".mkv", ".rmvb", ".rm", ".avi", ".mov")
        movie_names = subprocess.check_output(["ls", movie_folder]).decode("utf-8").split("\n")
        movie_names = [i for i in movie_names if i.endswith(movie_suffix)]
        # self.movie_name_list = movie_names
        return self.movie_name_list

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

    def Movie_To_Train_Ipca(self,movie_name_list=None):

        if movie_name_list is None:
            for movie_name in self.movie_name_list:
                self.Ipca_Train(movie_name)
        else:
            for movie_name in movie_name_list:
                self.Ipca_Train(movie_name)

    def Ipca_Train(self, movie_name,to_train=True):

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

            dict_list = [list(one_dict.values())[:-2] for one_dict in cursor_dict]
            # target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]

            if to_train:
                # train ipca chunk by chunk
                self.Ipca.partial_fit(dict_list)

                if i % 3 == 0 and self.reset_pca:
                    with open(self._model_path, 'wb') as file_id:
                        pickle.dump(self.Ipca, file_id)
            else:
                pass

            i += 1  # increase while

    def Ipca_Predict(self):
        with open(self._model_path, 'rb') as file_id:
            Ipca_loaded = pickle.load(file_id)
            self.Ipca_loaded = Ipca_loaded
        # test_data_ipcad_loaded = IPCA.transform(test_data)


if __name__ == "__main__":

    data_base_of_raw_data = DATABASE()
    print(data_base_of_raw_data.collections_of_eachdatabase)
    data_base_of_raw_data.database_chose("bar")
    data_base_of_raw_data.collection_chose("raw_vector01")
    print(data_base_of_raw_data.collection)

    bar = VECTORS_REDUCE(data_base_of_raw_data, movie_name_list=[3])
    bar.Movie_To_Train_Ipca()
