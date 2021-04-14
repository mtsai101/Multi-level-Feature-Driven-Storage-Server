from entropy import conv_entropy
import numpy as np
import tensorflow as tf
physical_devices = tf.config.list_physical_devices('GPU')
# tf.config.experimental.set_memory_growth(physical_devices[0], True)
tf.config.experimental.set_virtual_device_configuration(physical_devices[1], [
  tf.config.experimental.VirtualDeviceConfiguration(memory_limit=2048)
])

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