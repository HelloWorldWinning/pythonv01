from database import DATABASE
from model3 import MODEL_JPG_VECTOR


if __name__ =="__main__":
    print(" begining ".center(60,"="))
    folder_path = "/data/bar03/output"
    data_in_out = DATABASE()
    data_in_out.database_chose("bar2")
    data_in_out.collection_chose("bar2")
    #
    MODEL_JPG_VECTOR(folder_path = folder_path,database=data_in_out)
    # data = b.Jpg_To_Vector_DataBase(to_database=True)
    # print(img_vect.img_path_many)
