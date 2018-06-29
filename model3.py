import numpy as np
import subprocess
import os
# np.set_printoptions(precision=20)

from database import DATABASE

bar3 = DATABASE()
bar3.database_chose("bar3")
bar3.collection_chose("bar3")

class MODEL_JPG_VECTOR(object):

    def __init__(self, chunk=1500, img_path_many=None, folder_path=None,database=bar3):
        self.database = database

        from keras.models import Model
        self._Model = Model
        from keras.preprocessing import image
        self._image = image
        from keras.applications.xception import Xception as key_model
        self._key_model = key_model
        from keras.applications.xception import preprocess_input, decode_predictions
        self._preprocess_input = preprocess_input
        self._decode_predictions = decode_predictions
        base_model_4 = key_model(weights='imagenet', include_top=False)
        self._base_model_4 = base_model_4
        model = Model(inputs=base_model_4.input, outputs=base_model_4.get_layer(index=-3).output)
        self._model = model

        self.chunk = chunk

        if img_path_many == folder_path == None:
            raise Exception(" choice img_path_many or folder_path")
        elif img_path_many != None:
            self.img_path_many = img_path_many
        elif folder_path != None:
            #             self.folder_path = folder_path
            self.folder_path = folder_path
            self.img_path_many = self._Img_List()
        else:
            raise Exception("at least specify img_path_many or folder_path")

        self._Get_Target()

    def _Img_List(self):
        folder_path = self.folder_path
        movies_jpgs = subprocess.check_output(["ls", folder_path]).decode("utf-8").split("\n")
        movies_jpgs = [os.path.join(folder_path, i) for i in movies_jpgs if i.endswith(".jpg")]
        # print("movies_jpgs = " ,movies_jpgs)
        # img_path_many = movies_jpgs[10003:10000+5+5+3]
        img_path_many = movies_jpgs
        # print(img_path_many)
        if isinstance(img_path_many,str):
            img_path_many = [img_path_many]
        return img_path_many

    def Jpg_To_Vector(self, img_path):
        if isinstance(img_path,list):
            img_path = img_path[0]

        self.a1_img_path = img_path
        img = self._image.load_img(img_path, target_size=(299, 299))
        x = self._image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = self._preprocess_input(x)
        # self.a4_preprocessed = x
        output_of_model = self._model.predict(x, batch_size=1)
        # self.a5_output = output_of_model
        self.a6_data = output_of_model.reshape(1, -1)

        return self.a6_data,self._Get_Target(outside_img_path=img_path)

    def Jpg_To_Vector_DataBase(self,img_path_many=None,to_database=None):

        if  to_database== None:
            raise Exception(" select pump data to_database= <True>/<False> ")

        if img_path_many ==None:
            img_path_many = list(self.img_path_many)
        elif isinstance(img_path_many,str):
            # print("str or nor ".center(50,"%"))
            img_path_many = [img_path_many]
        elif isinstance(img_path_many,list):
            pass
        else:
            raise Exception(" img_path_many must be  a list containing  a image path(s)   ")

        chunk = self.chunk

        leng = len(img_path_many)

        i = 0
        while i * chunk < leng:
            # print("=" * 30)
            start_of_slice = i * chunk
            end_of_slice = min((i + 1) * chunk, leng)

            if (leng - (i + 1) * chunk) <= 0:
                temp_chunk = leng - i * chunk
            else:
                temp_chunk = chunk

            name_index = 0

            temp_zero_array = np.zeros([temp_chunk, 299, 299, 3], dtype=np.float32)

            for index, img_path in enumerate(img_path_many[start_of_slice:end_of_slice]):
                # self.b1_img_path = img_path
                img = self._image.load_img(img_path, target_size=(299, 299))
                # self.b_image = img
                x = self._image.img_to_array(img)
                # self.b2_x_array = x
                x = np.expand_dims(x, axis=0)
                temp_zero_array[index] = x
                # print("name_index =", name_index, "temp_chunk =", temp_chunk, "temp_zero_array.shape =", temp_zero_array.shape)
                name_index += 1

            temp_zero_array /= 127.5
            temp_zero_array -= 1.

            output_of_model = self._model.predict(temp_zero_array, batch_size=1)
            # self.b5_output = output_of_model
            data = output_of_model.reshape(temp_chunk, -1)
            # self.b6_data = data
            # print("data.shape =",data.shape, self.target[start_of_slice:end_of_slice].shape)

            i += 1

            temp_target = self.target[start_of_slice: end_of_slice]

            if to_database:
                # print("to database")
                print("chunk =", i-1, data.shape, temp_target.shape)
                # return data,temp_target
                print("  inserting data  ".center(40,"="))
                self.database.insert_data(data,temp_target)
                print("  inserted  ".center(80,"$"))

            else:
                return data, temp_target
                # self.database.insert_data(data,temp_target)
                # print("to query")
                # print(data.shape, temp_target.shape )
                # return data.shape ,temp_target.shape

    def _Get_Target(self, outside_img_path = None):
        if outside_img_path is not None:
            if isinstance(outside_img_path,str):
                img_path_many = [outside_img_path]
            elif isinstance(outside_img_path,list):
                img_path_many = outside_img_path
            else:
                raise Exception("input is not right")
        else:
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

        if outside_img_path != None:
            return name_sec
        self.target = name_sec



# folder_path = "/data/bar03/output"

# bar = MODEL_JPG_VECTOR(folder_path=folder_path)

# ten_jpg = bar.Jpg_To_Vector_DataBase(to_database=False)
# one_jpg = bar.Jpg_To_Vector(bar.img_path_many[-2])


# one_jpg_1 = bar.Jpg_To_Vector_DataBase(img_path_many=bar.img_path_many[-1],to_database=True)
# print(  "print( ( one_jpg.flatten() == one_jpg_1.flatten() ).all() )".center(1)  )
# print( ( one_jpg.flatten() ==ten_jpg[-1].flatten() ).all() )
