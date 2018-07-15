from database import DATABASE
import pickle

img_path = "/data/bar03/screenshot01/0_160.jpg"

import numpy as np
from keras.models import Model
from keras.preprocessing import image
from keras.applications.xception import Xception as key_model
from keras.applications.xception import preprocess_input, decode_predictions

base_model_4 = key_model(weights='imagenet', include_top=False)
model = Model(inputs=base_model_4.input, outputs=base_model_4.get_layer(index=-3).output)

def Jpg_To_Vector(img_path):
    if isinstance(img_path, list):
        img_path = img_path[0]

    img =image.load_img(img_path, target_size=(299, 299))
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0)
    x =preprocess_input(x)
    # self.a4_preprocessed = x
    output_of_model = model.predict(x, batch_size=1)
    # self.a5_output = output_of_model
    a6_data = output_of_model.reshape(1, -1)
    return a6_data

raw_vector = Jpg_To_Vector(img_path)

model_path = "/data/bar03/ipcav08.pkl"

with open(model_path, 'rb') as file_id:
    Ipca_loaded = pickle.load(file_id)

reduced_data = Ipca_loaded.transform(raw_vector)


reduced_database = DATABASE()
reduced_database.database_chose("bar")
reduced_database.collection_chose("raw_vector01_redu")

data_from_database = reduced_database.get_data(movie_name=0)
compare_data = data_from_database[:,:-2]
compare_target = data_from_database[:,-2:]

