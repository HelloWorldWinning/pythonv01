import time

import os
from database import DATABASE
import subprocess
import pickle
import faiss
import numpy as np
from keras.models import Model
from keras.preprocessing import image
from keras.applications.xception import Xception as key_model
from keras.applications.xception import preprocess_input, decode_predictions

base_model_4 = key_model(weights='imagenet', include_top=False)
model = Model(inputs=base_model_4.input, outputs=base_model_4.get_layer(index=-3).output)

class MATCH():


    def __init__(self,
                 reduced_database_source = "bar",
                 reduced_collection_source = "raw_vector01_redu",
                 folder_contains_imgs = "/data/bar03/screenshot01/",
                 traind_ipca_model_path = "/data/bar03/ipcav08.pkl",
                 index_to_name_file = "/data/bar03/moive_name_list_new.txt"
                 ):

        reduced_database = DATABASE()
        reduced_database.database_chose(reduced_database_source)
        reduced_database.collection_chose(reduced_collection_source)
        data_from_database = reduced_database.get_data().astype("float32")

        self.compare_data = data_from_database[:, :-2]
        self.compare_target = data_from_database[:, -2:]

        self.folder_contains_imgs = folder_contains_imgs
        self.img_path_many = self._Img_List()
        self.model_path_ipca = traind_ipca_model_path
        with open(self.model_path_ipca, 'rb') as file_id:
            self._Ipca_loaded = pickle.load(file_id)

        file = open(index_to_name_file, "r")
        # And for reading use
        lines = file.read().split()
        file.close()
        self.index_to_name = lines
        self._Query_Index_Builder()

        del self.compare_data


    def _Img_List(self,folder_path_input=None):

        if folder_path_input is None:
            folder_path = self.folder_contains_imgs
        else:
            folder_path = folder_path_input

        movies_jpgs = subprocess.check_output(["ls", folder_path]).decode("utf-8").split("\n")
        movies_jpgs = [os.path.join(folder_path, i) for i in movies_jpgs if i.endswith(".jpg")]
        print("movies_jpgs =", movies_jpgs)
        img_path_many = movies_jpgs
        if isinstance(img_path_many, str):
            img_path_many = [img_path_many]

        return img_path_many


    def Many_Query_Data(self, img_path_many_input= None):
        if img_path_many_input is None:
            img_path_many = self.img_path_many
        else:
            img_path_many = img_path_many_input

        temp_chunk = len(img_path_many)
        temp_zero_array = np.zeros([temp_chunk, 299, 299, 3], dtype=np.float32)

        for index, img_path in enumerate(img_path_many):
            # self.b1_img_path = img_path
            img = image.load_img(img_path, target_size=(299, 299))
            # self.b_image = img
            x = image.img_to_array(img)
            # self.b2_x_array = x
            x = np.expand_dims(x, axis=0)
            temp_zero_array[index] = x

        temp_zero_array /= 127.5
        temp_zero_array -= 1.
        output_of_model = model.predict(temp_zero_array, batch_size=1)
        # self.b5_output = output_of_model
        data = output_of_model.reshape(temp_chunk, -1)
        # self.data = data
        # bar = MATCH()
        # bar.Many_Query_Data()
        # raw_vector = Jpg_To_Vector(img_path)
        # model_path = self.model_path_ipca

        reduced_data = self._Ipca_loaded.transform(data)
        return reduced_data


    def Jpg_To_Vector_Reduced(self,img_path):

        if isinstance(img_path, list):
            img_path = img_path[0]

        img =image.load_img(img_path, target_size=(299, 299))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x =preprocess_input(x)
        output_of_model = model.predict(x, batch_size=1)
        a6_data = output_of_model.reshape(1, -1)
        reduced_data = self._Ipca_loaded.transform(a6_data)
        return reduced_data

        # match = MATCH()
        # match.compare_target
        # match.compare_data

    def _Query_Index_Builder(self):

        d = 160                               # dimension
        nb = self.compare_target.shape[0]     # database size
        # nq = 2                                # nb of queries

        xb = self.compare_data.astype("float32")
        xb[:, 0] += np.arange(nb) / 1000.

        # xq = self.Many_Query_Data().astype("float32")
        # xq[:, 0] += np.arange(nq) / 1000.

        # index = faiss.IndexFlatL2(d)   # build the index
        self.data_base_index = faiss.IndexFlatL2(d)
        print(self.data_base_index.is_trained)

        self.data_base_index.add(np.ascontiguousarray(xb))                  # add vectors to the index

        print(self.data_base_index.ntotal)


    def Queury_Name_Time(self, folder_containing_imgs):

        img_path_many = self._Img_List(folder_containing_imgs)
        reduced_data = self.Many_Query_Data(img_path_many)

        nq = len(reduced_data)                           # nb of queries
        xq = reduced_data.astype("float32")
        xq[:, 0] += np.arange(nq) / 1000.
        k = 1                            # we want to see 4 nearest neighbors
        t0 = time.time()
        D, I = self.data_base_index.search(xq, k)       # sanity check
        t1 = time.time()
        print("Query time is  ={} ".format(t1-t0))
        # print(I)
        # print(D)
        # print(compare_target[ [i for i in I.flatten()] ] )

        print("$"*80)
        # print(self.compare_target[ [i for i in I.flatten()] ])

        print( "len(self.compare_target)   =", len(self.compare_target) )


        movie_name_index = self.compare_target[:,0][ [i for i in I.flatten()] ]
        movie_names_list = [self.index_to_name[int(i)] for i in movie_name_index]

        movie_time_index = self.compare_target[:,1][ [i for i in I.flatten()] ].tolist()

        print(movie_names_list)
        print(movie_time_index)
        return movie_names_list, movie_time_index


match =MATCH()

folder = "/data/bar03/screenshot01"
match.Queury_Name_Time(folder)



# very high effiency model

# d = 160                               # dimension
# nb = match.compare_target.shape[0]    # database size
# nq = 1                                # nb of queries
# # np.random.seed(1234)                # make reproducible
#
# xb = match.compare_data.astype("float32")
# xb[:, 0] += np.arange(nb) / 1000.
#
# xq = match.Many_Query_Data().astype("float32")
# xq[:, 0] += np.arange(nq) / 1000.
#
# index = faiss.IndexFlatL2(d)   # build the index
# print(index.is_trained)
# index.add(np.ascontiguousarray(xb))                  # add vectors to the index
# print(index.ntotal)
#
# k = 2                            # we want to see 4 nearest neighbors
# D, I = index.search(xq, k)       # sanity check
#
# # print(I)
# # print(D)
# # print(compare_target[ [i for i in I.flatten()] ] )
#
# print("$"*80)
# print(match.compare_target[ [i for i in I.flatten()] ])
#
# movie_name_index =  match.compare_target[:,0][ [i for i in I.flatten()] ]
#
# [lines[int(i)] for i in movie_name_index]
