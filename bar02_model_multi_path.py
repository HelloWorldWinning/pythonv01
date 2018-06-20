import numpy as np
import subprocess
import pandas as pd
import os

class MODEL_JPG_VECTOR(object):

    def __init__(self, chunk=1500, img_path_many=None, folder_path=None):
        from keras.models import Model
        self._Model = Model
        from keras.preprocessing import image
        self._image = image
        from keras.applications.xception import Xception as key_model
        self._key_model = key_model
        from keras.applications.xception import preprocess_input, decode_predictions
        self._preprocess_input = preprocess_input
        self._decode_predictions = decode_predictions
        self._base_model_4 = self._key_model(weights='imagenet', include_top=False)
        # self._base_model_4 = self._base_model_4
        self._model = self._Model(inputs=self._base_model_4.input, outputs=self._base_model_4.get_layer(index=-3).output)
        # self._model = model

        self.chunk = chunk

        if img_path_many == folder_path == None:
            raise Exception(" choice img_path_many or folder_path")
        elif img_path_many != None:
            self.img_path_many = img_path_many
        elif folder_path != None:
            self.folder_path = folder_path
            self.img_path_many = self._Img_List()
        else:
            raise Exception("at least specify img_path_many or folder_path")

        self.Get_Target()

    def _Img_List(self):
        folder_path = self.folder_path
        movies_jpgs = subprocess.check_output(["ls", folder_path]).decode("utf-8").split("\n")
        movies_jpgs = [os.path.join(folder_path, i) for i in movies_jpgs if i.endswith(".jpg")]
        img_path_many = movies_jpgs[100:103 ]
        print(img_path_many)
        return img_path_many

    def Jpg_To_Vector(self, img_path):

        self.a1_img_path = img_path
        img = self._image.load_img(img_path, target_size=(299, 299))
        x = self._image.img_to_array(img)
        self.a_1 = x
        x = np.expand_dims(x, axis=0)
        self.a_2 = x
        x = self._preprocess_input(x)
        self.a_3 = x
        output_of_model = self._model.predict(x, batch_size=32 * 5)
        self.a6_data = output_of_model.reshape(1, -1)
        return self.a6_data

    def Jpg_To_Vector_DataBase(self):

        chunk = self.chunk
        img_path_many = self.img_path_many
        for i in img_path_many:
            print("img_path_many", i)
        leng = len(img_path_many)

        i = 0
        while i * chunk < leng:
            print("=" * 30)
            start_of_slice = i * chunk
            end_of_slice = min((i + 1) * chunk, leng)

            if (leng - (i + 1) * chunk) <= 0:
                temp_chunk = leng - i * chunk
            else:
                temp_chunk = chunk

            name_index = 0

            temp_zero_array = np.ones([temp_chunk, 299, 299, 3], dtype=np.float32)
            for index, img_path in enumerate(img_path_many[start_of_slice:end_of_slice]):

                self.b1_img_path = img_path
                img = self._image.load_img(img_path, target_size=(299, 299))

                self.b_image = img
                x = self._image.img_to_array(img)
                self.b_1 = x
                # self.b2_x_array = x
                x = np.expand_dims(x, axis=0)
                self.b_2 = x
                # self.b3_x_expand = x

                temp_zero_array[index] = x

                print("name_index =", name_index, "temp_chunk =", temp_chunk, "temp_zero_array.shape =",
                      temp_zero_array.shape)
                name_index += 1

            # self.b3_x_expand_temp_zero_array = temp_zero_array
            x = self._preprocess_input(temp_zero_array)

            self.b_3 = x
            # self.b4_preprocessed = x

            output_of_model = self._model.predict(x, batch_size=32 * 5)
            self.b_last_output = output_of_model
            # self.b5_output = output_of_model
            data = output_of_model.reshape(temp_chunk, -1)
            # self.b6_data = data
            # print(data)
            # print(data.shape, self.target[start_of_slice:end_of_slice].shape)
            print("data.shape =",data.shape,"self.target[start_of_slice:end_of_slice].shape =", self.target[start_of_slice:end_of_slice].shape)
            i += 1
        self.b_data = data
        return data

    def Get_Target(self):
        img_path_many = self.img_path_many
        name_fps = np.zeros([len(img_path_many), 2])
        frames_location = np.zeros(len(img_path_many))
        for index, str_i in enumerate(img_path_many):
            name_fps_list = str_i.split('/')[-1].split('__split__split__')[:2]
            name_fps[index] = np.array([float(i) for i in name_fps_list])
            frames_tmp = str_i.split('/')[-1].split('__split__split__')[-1].split(".jpg")[0]
            frames_location[index] = np.array(float(frames_tmp))
        name_sec = name_fps
        name_sec[:, 1] = frames_location / name_fps[:, 1]
        self.target = name_sec



folder_path = "/data/bar03/output"

bar = MODEL_JPG_VECTOR(folder_path=folder_path)
one_jpg = bar.Jpg_To_Vector(bar.img_path_many[-1])
# one_jpg
ten_jpg = bar.Jpg_To_Vector_DataBase()
# ten_jpg
# print((bar.b3_x_expand == bar.a3_x_expand).all(), "(bar.b3_x_expand == bar.a3_x_expand).all()")
print(  "(one_jpg == ten_jpg[-1]).all() =" ,(one_jpg.flatten() == ten_jpg[-1].flatten()).all()  )
