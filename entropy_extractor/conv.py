import tensorflow as tf
from tensorflow.keras import datasets, layers, models
from entropy import conv_entropy
import numpy as np
import time
import cv2

gpu = tf.config.experimental.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(gpu[0], True)


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
def get_conv_entropy(input_file):

    total_entropy = 0
    vs = cv2.VideoCapture(input_file)
    
    while True:
        ret, frame = vs.read()
        if ret is False:
          break
        frame = tf.image.resize(frame, [512,512], method='bilinear')
        frame = tf.expand_dims(frame, axis=0)/255.0
        conv_feature = simpleConv(frame)
        # print(conv_feature)
        entropy_value=0
        for channel in range(4):
            signal = tf.keras.backend.flatten(conv_feature[:,:,:,channel]).numpy().round(decimals=3)
            total_entropy += conv_entropy(signal)

    return total_entropy

if __name__=="__main__":
    input_file = "/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4"
    s = time.time()
    total_entropy = get_conv_entropy(input_file)
    print(time.time()-s)
    print(total_entropy)