from database import DATABASE
import time

print(DATABASE().collections_of_eachdatabase)

bar = DATABASE()
bar.database_chose("bar")
bar.collection_chose("raw_vector01_redu")

print(bar.collection.count())



# while True:
#     print(  bar.collection.count() )
#     time.sleep(60)