from entropy import conv_entropy
import numpy as np
import time
import cv2
import tensorflow as tf
from tensorflow.keras import datasets, layers, models
gpus = tf.config.experimental.list_physical_devices('GPU')
tf.config.experimental.set_virtual_device_configuration(gpus[0], [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=1024)])


class SimpleConv(tf.keras.Model):
  def __init__(self):
    super(SimpleConv, self).__init__(name='')

    self.conv2a = tf.keras.layers.Conv2D(64, 3, strides=(1, 1), kernel_initializer='glorot_uniform', padding='valid', data_format=None, activation='relu')
    self.mp2a = tf.keras.layers.MaxPooling2D()

    self.conv2b = tf.keras.layers.Conv2D(32, 3, strides=(1, 1), kernel_initializer='glorot_uniform', padding='valid', data_format=None, activation='relu')
    self.mp2b = tf.keras.layers.MaxPooling2D()

    self.conv2c = tf.keras.layers.Conv2D(16, 3, strides=(1, 1), kernel_initializer='glorot_uniform', padding='valid', data_format=None, activation='relu')
    self.mp2c = tf.keras.layers.MaxPooling2D()

    self.conv2d = tf.keras.layers.Conv2D(4, 3, strides=(1, 1), kernel_initializer='glorot_uniform', padding='valid', data_format=None, activation='softmax')
    self.mp2d = tf.keras.layers.MaxPooling2D()

  def call(self, input_tensor):
    x = self.conv2a(input_tensor)
    x = self.mp2a(x)
 
    x = self.conv2b(x)
    x = self.mp2b(x)

    x = self.conv2c(x)
    x = self.mp2c(x)

    x = self.conv2d(x)
    x = self.mp2d(x)

    return x

simpleConv = SimpleConv()
def get_conv_entropy(input_file, return_value):
  total_entropy = 0
  vs = cv2.VideoCapture(input_file)
  frame_count = 0
  while True:
    ret, frame = vs.read()
    if ret is False:
      break
    if frame_count%24==0:
      frame = tf.image.resize(frame, [512,512], method='bilinear')
      frame = tf.expand_dims(frame, axis=0)/255.0
      conv_feature = simpleConv(frame)
      for channel in range(4):
          signal = tf.keras.backend.flatten(conv_feature[:,:,:,channel]).numpy().round(decimals=3)
          total_entropy += conv_entropy(signal)
    frame_count += 1
  return_value.value = total_entropy

# if __name__=="__main__":
#     input_file = "/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4"
#     s = time.time()
#     total_entropy = get_conv_entropy(input_file)
#     print(time.time()-s)
#     print(total_entropy)