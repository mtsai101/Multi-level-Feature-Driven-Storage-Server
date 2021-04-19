from entropy import conv_entropy
import numpy as np
import tensorflow as tf
import subprocess as sp
import os

def get_gpu_with_most_memory():
    _output_to_list = lambda x: x.decode('ascii').split('\n')[:-1]

    ACCEPTABLE_AVAILABLE_MEMORY = 1024
    COMMAND = "nvidia-smi --query-gpu=memory.free --format=csv"
    memory_free_info = _output_to_list(sp.check_output(COMMAND.split()))[1:]
    memory_free_values = [int(x.split()[0]) for i, x in enumerate(memory_free_info)]
    
    return np.argmax(memory_free_values)

use_gpu_id = get_gpu_with_most_memory()
physical_devices = tf.config.list_physical_devices('GPU')
tf.config.set_visible_devices(physical_devices[use_gpu_id], 'GPU')
tf.config.experimental.set_memory_growth(physical_devices[use_gpu_id], True)

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