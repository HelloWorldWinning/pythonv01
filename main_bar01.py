from database import DATABASE
from model3 import MODEL_JPG_VECTOR


if __name__ =="__main__":
    print(" begining ".center(60,"="))
    folder_path = "/data/bar04/output"
    data_in_out = DATABASE()
    data_in_out.database_chose("bar")
    data_in_out.collection_chose("raw_vector02")

    data_to_mongod = MODEL_JPG_VECTOR(chunk=400,folder_path = folder_path,database=data_in_out,)
    data_to_mongod.Jpg_To_Vector_DataBase(to_database=True)
    # print(data_to_mongod.img_path_many.__len__())
    # data = data_in_out.get_data()
