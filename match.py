from database import DATABASE
import pickle
import faiss


import numpy as np
from keras.models import Model
from keras.preprocessing import image
from keras.applications.xception import Xception as key_model
from keras.applications.xception import preprocess_input, decode_predictions




base_model_4 = key_model(weights='imagenet', include_top=False)
model = Model(inputs=base_model_4.input, outputs=base_model_4.get_layer(index=-3).output)


reduced_database = DATABASE()
reduced_database.database_chose("bar")
reduced_database.collection_chose("raw_vector01_redu")

data_from_database = reduced_database.get_data(movie_name=0).astype("float32")
compare_data = data_from_database[:,:-2]
compare_target = data_from_database[:,-2:]


img_path = "/data/bar03/screenshot01/0_160.jpg"


def Jpg_To_Vector(img_path):
    if isinstance(img_path, list):
        img_path = img_path[0]

    img =image.load_img(img_path, target_size=(299, 299))
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0)
    x =preprocess_input(x)
    output_of_model = model.predict(x, batch_size=1)
    a6_data = output_of_model.reshape(1, -1)
    return a6_data

raw_vector = Jpg_To_Vector(img_path)

model_path = "/data/bar03/ipcav08.pkl"

with open(model_path, 'rb') as file_id:
    Ipca_loaded = pickle.load(file_id)

reduced_data = Ipca_loaded.transform(raw_vector)






# very high effiency model

d = 160                           # dimension
nb = compare_target.shape[0]        # database size
nq = 1                             # nb of queries
# np.random.seed(1234)             # make reproducible
xb = compare_data
xb[:, 0] += np.arange(nb) / 1000.
xq = reduced_data.astype("float32")
xq[:, 0] += np.arange(nq) / 1000.

index = faiss.IndexFlatL2(d)   # build the index
print(index.is_trained)
index.add(np.ascontiguousarray(xb))                  # add vectors to the index
print(index.ntotal)


k = 3                          # we want to see 4 nearest neighbors
D, I = index.search(xq, k)     # sanity check
print(I)
print(D)
print(compare_target[ [i for i in I.flatten()] ] )















#
# def Jpg_To_Vector(img_path):
#     if isinstance(img_path, list):
#         img_path = img_path[0]
#
#     img =image.load_img(img_path, target_size=(299, 299))
#     x = image.img_to_array(img)
#     x = np.expand_dims(x, axis=0)
#     x =preprocess_input(x)
#     # self.a4_preprocessed = x
#     output_of_model = model.predict(x, batch_size=1)
#     # self.a5_output = output_of_model
#     a6_data = output_of_model.reshape(1, -1)
#     return a6_data
#
# raw_vector = Jpg_To_Vector(img_path)
#
# model_path = "/data/bar03/ipcav08.pkl"
#
# with open(model_path, 'rb') as file_id:
#     Ipca_loaded = pickle.load(file_id)
#
# reduced_data = Ipca_loaded.transform(raw_vector)
#
#
# reduced_database = DATABASE()
# reduced_database.database_chose("bar")
# reduced_database.collection_chose("raw_vector01_redu")
#
# data_from_database = reduced_database.get_data(movie_name=0).astype("float32")
# compare_data = data_from_database[:,:-2]
# compare_target = data_from_database[:,-2:]
#
#
#
# # very high effiency model
#
# d = 160                           # dimension
# nb = compare_target.shape[0]        # database size
# nq = 1                             # nb of queries
# # np.random.seed(1234)             # make reproducible
# xb = compare_data
# xb[:, 0] += np.arange(nb) / 1000.
# xq = reduced_data.astype("float32")
# xq[:, 0] += np.arange(nq) / 1000.
#
# index = faiss.IndexFlatL2(d)   # build the index
# print(index.is_trained)
# index.add(np.ascontiguousarray(xb))                  # add vectors to the index
# print(index.ntotal)
#
#
# k = 3                          # we want to see 4 nearest neighbors
# D, I = index.search(xq, k)     # sanity check
# print(I)
# print(D)
# print(compare_target[ [i for i in I.flatten()] ] )
