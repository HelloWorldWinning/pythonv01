from database import DATABASE



if __name__ =="__main__":
    print(" begining ".center(60,"%"))
    data_in_out = DATABASE()
    data_in_out.database_chose("bar2")
    data_in_out.collection_chose("bar2")
