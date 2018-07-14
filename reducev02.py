import time
import pickle
import subprocess
from database import DATABASE
import numpy as np
import pymongo

class VECTORS_REDUCE():

    def __init__(self, data_base_of_raw_data =None, data_base_reduced =None,
                 folder_containing_movies=None,
                 movie_name_list=None,
                 n_components=160, batch_size=600,
                 ipca_model_path=None,
                 reset_pca=True,
                 # to_train=True,
                 movie_name_int_path = "/data/bar03/moive_name_list_new.txt"
                 ):
        self.movie_name_int_path = movie_name_int_path
        if data_base_of_raw_data is not None:
            self.data_base = data_base_of_raw_data
        if data_base_reduced is not None:
            self.data_base_reduced = data_base_reduced

        self.second = 10 * 60 * 60
        self.n_components = n_components
        self.batch_size = batch_size
        # if not isinstance(to_train,bool):
        #     raise ValueError(
        #             """ to_train =True means to train pca data
        #                 to_train =False means to reduce data's dimension ,so to get feature.
        #                 you must decide to train a ipca model or
        #                 reduce data's dimention to feature data base."
        #             """)
        # self.to_train = to_train
        # if not self.to_train:

        # Load Trained_Pic Model
        try:
            self.Ipca_Reduced_Model_Load()
        except:
            print(" no pca model to load")

        # Reset Pca
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
                "You should specify a folder that contains movie files. Or a list that contains a movie names"
                           )

        if ipca_model_path is None:
            self._model_path = "/data/bar03/ipcav06.pkl"
        else:
            self._model_path = ipca_model_path

    def _Get_Moive_Name_From_Folder(self, movie_folder=None):

        if movie_folder is None:
            movie_folder = self._movie_folder

        movie_suffix = (".3gp", ".mp4", ".mkv", ".rmvb", ".rm", ".avi", ".mov")
        movie_names = subprocess.check_output(["ls", movie_folder]).decode("utf-8").split("\n")
        movie_names = [i for i in movie_names if i.endswith(movie_suffix)]
        # self.movie_name_list = movie_names
        # todo_2  movie_name_list to movie_int_list
        with open(self.movie_name_int_path, encoding="utf-8") as file:
            movie_name_list = [l.strip() for l in file]
        self.movie_name_list = [ movie_name_list.index(i) for i  in movie_names]

        return self.movie_name_list

    def _Moive_Name_List(self, movie_name_list):

        if isinstance(movie_name_list, str):
            movie_name_list = [movie_name_list]

        self.movie_name_list = movie_name_list

        return self.movie_name_list

    def _Set_Ipca(self, n_components=None, batch_size=None, copy=False):

        if n_components is None:
            n_components = self.n_components
        if batch_size is None:
            batch_size = self.batch_size

        from sklearn.decomposition import IncrementalPCA
        IPCA = IncrementalPCA(n_components=n_components, batch_size=batch_size, copy=copy)
        self.Ipca = IPCA
        return self.Ipca

    def _Dynamic_Chunk_Time(self, movie_name=None, chunk_min=180, chunk_max=400, max_second=100):

        """ return chunk_time second and chunk block count
        """
        if movie_name is None:
            raise ValueError(" there is no movie_name")

        data_base = self.data_base

        for chunk_time in np.arange(2, max_second,0.5):
            left = 0
            right = chunk_time
            cursor = data_base.collection.find(
                {"movie_name": movie_name, "second": {"$gte": left, "$lt": right}},
                {"_id": True},  # "second":True,
                batch_size=1000,
                       # cursor_type= pymongo.CursorType.EXHAUST
            )
            if cursor.count() >= chunk_min and cursor.count() <= chunk_max:
                #             print("chunk_time =",chunk_time,"cursor.count() =",cursor.count())
                self.chunk_time = chunk_time
                self.chunk_count = cursor.count()
                print( "self.chunk_time, self.chunk_count" ,self.chunk_time, self.chunk_count)
                return self.chunk_time, self.chunk_count

        raise Exception("can not get time chunk_time second")

    #     chunk_time,chunk_count = Dynamic_Chunk_Time(movie_name=movie_name)

    def Movie_To_Train_Ipca(self, movie_name_list=None):

        if movie_name_list is None:
            for movie_name in self.movie_name_list:
                self.Ipca_Train_Reduce(movie_name,to_reduce_data=False)
        else:
            for movie_name in movie_name_list:
                self.Ipca_Train_Reduce(movie_name,to_reduce_data=False)

    def Data_To_Feature(self, movie_name_list =None,):

        if movie_name_list is None:
            for movie_name in self.movie_name_list:
                self.Ipca_Train_Reduce(movie_name=movie_name,to_reduce_data=True)
        else:
            for movie_name in movie_name_list:
                self.Ipca_Train_Reduce(movie_name=movie_name,to_reduce_data=True)


    def Ipca_Train_Reduce(self, movie_name, to_reduce_data = None):

        data_base = self.data_base
        chunk_time, chunk_count = self._Dynamic_Chunk_Time(movie_name=movie_name)

        i = 0
        while True:
            print("  once chunk_time  ".center(90, "="))

            left = i * chunk_time
            right = (i + 1) * chunk_time

            # todo 1  what data you want

            cursor = data_base.collection.find (
                {"movie_name": movie_name, "second": {"$gte": left, "$lt": right}},
                {"_id": False},  # "second": True,
                batch_size = 1000,
                cursor_type = pymongo.CursorType.EXHAUST)

            print("  cursor.count()   =  {}".format(cursor.count()).center(50, "*"))

            if cursor.count() ==0:
                print("        cursor.count() ==0       ")
                break
            # if cursor.count() == 0:
            #     print(" cursor.count() == 0 ")
            #     break

            t0 = time.time()
            cursor_dict = list(cursor)
            cursor.close()

            # dict_list = [list(one_dict.values()) for one_dict in cursor_dict]

            print("   read data time = {} ".format(time.time()-t0).center(90, "*"))

            data_list = [list(one_dict.values())[:-2] for one_dict in cursor_dict]
            target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]

            print("len(dict_list) =", len(data_list),
                  "len(dict_list[-1] =", len(data_list[-1]),
                  "target_list[-3:]",target_list[-3:]
                  )

            # To Train = not to_reduce_data
            if not to_reduce_data:
                if cursor.count() < self.n_components:
                    print("  cursor.count() < n_components  ")
                    break
                # if self.to_train:
                    # train ipca chunk by chunk
                print(" to train model, data length is = {} ,data dimension is = {}, target length = {}"
                      .format(len(data_list), len(data_list[-1]), len(target_list)))
                self.Ipca.partial_fit(data_list)

                if i % 10 == 0 and self.reset_pca:

                    with open(self._model_path, 'wb') as file_id:
                        pickle.dump(self.Ipca, file_id)

            # To Reduce Data
            if to_reduce_data:

                print(" to predict data, data length is = {} ,data dimension is = {}, target length = {}"
                      .format(len(data_list), len(data_list[-1]), len(target_list)))

                # To Reduce Data
                ipcaed_vector = self.Ipca_Loaded.transform(data_list)
                self.ipcaed_vector  =  ipcaed_vector
                print(" to predict data, data length is = {} ,data dimension is = {}, target length = {}"
                      .format(len(ipcaed_vector), len(ipcaed_vector[-1]), len(target_list)))

                # target_list = [list(one_dict.values())[-2:] for one_dict in cursor_dict]
                # todo_1
                # insert reduced data to reduced database


                # Insert Reduced data to database
                # self.data_base_reduced.insert_data(ipcaed_vector, target_list)
                self.data_base_reduced.insert_data(ipcaed_vector, target_list)

            i += 1  # increase while

    def Ipca_Reduced_Model_Load(self, model_path =None):
        if model_path is None:
            with open(self._model_path, 'rb') as file_id:
                Ipca_loaded = pickle.load(file_id)
                self.Ipca_Loaded = Ipca_loaded
        else:
            with open(model_path, 'rb') as file_id:
                Ipca_loaded = pickle.load(file_id)
                self.Ipca_Loaded = Ipca_loaded
        # test_data_ipcad_loaded = IPCA.transform(test_data)


if __name__ == "__main__":

    data_base_of_raw_data = DATABASE()
    data_base_of_raw_data.database_chose("bar")
    data_base_of_raw_data.collection_chose("raw_vector01")

    data_base_reduced = DATABASE()
    data_base_reduced.database_chose("bar")
    data_base_reduced.collection_chose("raw_vector01_redu")

    print(data_base_of_raw_data.collections_of_eachdatabase)

    train_ipca = VECTORS_REDUCE(data_base_of_raw_data = data_base_of_raw_data,
                           data_base_reduced = data_base_reduced,
                           # folder_containing_movies="/data/bar03",
                           movie_name_list = [3],
                           ipca_model_path ="/data/bar03/ipcav06.pkl",
                           reset_pca = True

                                )

    # print(train_ipca._Get_Moive_Name_From_Folder())
    # print(train_ipca.movie_name_list)

    # train_ipca.Movie_To_Train_Ipca()
    #
    # print("$"*100)
    train_ipca.Ipca_Reduced_Model_Load()
    train_ipca.Data_To_Feature()

    # vector_to_feature = VECTORS_REDUCE(  data_base_of_raw_data = data_base_of_raw_data,
    #                        data_base_reduced =data_base_reduced,
    #                        movie_name_list=[3],
    #                        to_train=False
    #                                      )
