import tensorflow as tf
import keras
def xx__yy():
    print("NOTHING HERE")

# from tensorflow.python.client import device_lib
# print(device_lib.list_local_devices())
# gpu_fraction = 0.1
# gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=gpu_fraction)
# sess = tf.Session(config=tf.ConfigProto(gpu_options=gpu_options))

# import tensorflow as tf
# with tf.device('/gpu:0'):
#   a = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[2, 3], name='a')
#   b = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[3, 2], name='b')
#   c = tf.matmul(a, b)
# sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))
# print (sess.run(c))
#
# form keras import  backend as K
# K.tens

from keras import backend as K
print( K.tensorflow_backend._get_available_gpus() )