class MODEL_JPG_VECTOR(object):

    def __init__(self, chunk=1500, img_path_many=None, folder_path=None):
        self.chunk = chunk

        if img_path_many == folder_path == None:
            raise Exception(" choce img_path_many or folder_path")
        elif img_path_many != None:
            self.img_path_many = img_path_many
        elif folder_path != None:
            #             self.folder_path = folder_path
            self.img_path_many = self.Img_List(folder_path)
        else:
            raise Exception("at least specify img_path_many or folder_path")

        self.Get_Target()

    def Img_List(self, folder_path):
        #         folder_path=self.folder_path
        movies_jpgs = subprocess.check_output(["ls", folder_path]).decode("utf-8").split("\n")
        # img_path = "output/"+movies_jpgs[8050+600+2+1000]
        # img_path_many.append(img_path)
        movies_jpgs = [i for i in movies_jpgs if i.endswith(".jpg")]

        #         movies_jpgs[-1]

        #         img_path_many_all=[]

        #         for i in movies_jpgs:
        #             img_path = "output/"+i
        #             img_path_many_all.append(img_path)
        img_path_many = movies_jpgs

        img_path_many = np.random.choice(img_path_many, 10)
        img_path_many = img_path_many.tolist()

        return img_path_many

    def Jpg_To_Vector(self):
        chunk = self.chunk
        img_path_many = self.img_path_many
        leng = len(img_path_many)
        index_end = int(leng / chunk)
        print("inde_end", index_end)
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

            #             print( type(self.target[start_of_slice:end_of_slice] )  )
            #             print(self.target[start_of_slice:end_of_slice].shape )

            temp_zero_array = np.zeros([temp_chunk, 224, 224, 3])

            for index, img_path in enumerate(img_path_many[start_of_slice:end_of_slice]):
                #         print(index,img_path,temp_chunk)
                img = image.load_img(img_path, target_size=(224, 224))
                x = image.img_to_array(img)
                x = np.expand_dims(x, axis=0)
                temp_zero_array[index] = x
                name_index += 1

            x = preprocess_input(temp_zero_array)
            output_of_model = model.predict(x, batch_size=32 * 5)
            data = output_of_model.reshape(len(x), -1)
            #             print(data.shape )
            #             print(data)

            #             print(data.shape,self.target[start_of_slice:end_of_slice].shape)
            # to database
            print(data.shape, self.target[start_of_slice:end_of_slice].shape)
            data_to_database.insert_data(data, self.target[start_of_slice:end_of_slice])
            #             self.bar_data = data
            #             self.bar_target = self.target[start_of_slice:end_of_slice]
            i += 1

    def Get_Target(self):
        img_path_many = self.img_path_many
        name_fps = np.zeros([len(img_path_many), 2])
        frames_location = np.zeros(len(img_path_many))
        for index, str_i in enumerate(img_path_many):
            name_fps_list = str_i.split('/')[-1].split('__split__split__')[:2]
            name_fps[index] = np.array([float(i) for i in name_fps_list])
            frames_tmp = str_i.split('/')[-1].split('__split__split__')[-1].split(".jpg")[0]
            frames_location[index] = np.array(float(frames_tmp))
        #     print(np.array(float(frames_tmp) ))
        name_sec = name_fps
        name_sec[:, 1] = frames_location / name_fps[:, 1]
        self.target = name_sec
