import tensorflow as tf
import cv2
import numpy as np
import time
from conv import SimpleConv
from entropy import conv_entropy

# def crnn(tensor, kernel_size, stride, out_channels, rnn_n_layers, rnn_type, bidirectional, padding):
#     # Expand to have 4 dimensions if needed
#     if len(tensor.shape) == 3:
#         tensor = tf.expand_dims(tensor, 3)

#     # Extract the patches (returns [batch, time-steps, 1, patch content flattened])
#     batch_size = tensor.shape[0]
#     n_in_features = tensor.shape[2]
#     patches = tf.image.extract_patches(images=tensor, 
#                              sizes=[1, kernel_size, n_in_features, 1], 
#                              strides=[1, stride, n_in_features, 1], 
#                              rates=[1, 1, 1, 1], 
#                              padding=padding)
    
#     patches = patches[:, :, 0, :]
    

#     # Reshape to do: 
#     # 1) reshape the flattened patches back to [kernel_size, n_in_features]
#     # 2) combine the batch and time-steps dimensions (which will be the new 'batch' size, for the RNN)
#     # now shape will be [batch * time-steps, kernel_size, n_features]
#     time_steps_after_stride = patches.shape[1]
#     patches = tf.reshape(patches, [batch_size * time_steps_after_stride, kernel_size, n_in_features])
    

#     # Transpose and convert to a list, to fit the tf.contrib.rnn.static_rnn requirements
#     # Now will be a list of length kernel_size, each element of shape [batch * time-steps, n_features]
#     patches = tf.transpose(patches, [1, 0, 2])


#     # Create the RNN Cell
#     if rnn_type == 'simple':
#         rnn_cell = tf.keras.layers.SimpleRNNCell
#     elif rnn_type == 'lstm':
#         rnn_cell = tf.keras.layers.LSTMCell
#     elif rnn_type == 'gru':
#         rnn_cell = tf.keras.layers.GRUCell

#     rnn_cell_func = rnn_cell(out_channels)
    
#     if not bidirectional:
#         layer = tf.keras.layers.RNN(rnn_cell_func, return_sequences=True, go_backwards=False)
#         outputs = layer(patches)
#     else:
#         forward_layer = tf.keras.layers.RNN(rnn_cell_func, return_sequences=True, return_state=False,go_backwards=False)
#         backward_layer = tf.keras.layers.RNN(rnn_cell_func, return_sequences=True, return_state=False,go_backwards=True)
#         layer = tf.keras.layers.Bidirectional(forward_layer, backward_layer=backward_layer)
#         outputs = layer(patches)
    
# #     # Multilayer RNN? (does not appear in the original paper)
# #     if rnn_n_layers > 1:
# #         if not bidirectional:
# #             rnn_cell = tf.compat.v1.nn.rnn_cell.MultiRNNCell([rnn_cell] * rnn_n_layers)
# #         else:
# #             rnn_cell_f = tf.compat.v1.nn.rnn_cell.MultiRNNCell([rnn_cell_f] * rnn_n_layers)
# #             rnn_cell_b = tf.compat.v1.nn.rnn_cell.MultiRNNCell([rnn_cell_b] * rnn_n_layers)
    


#     # Use only the output of the last time-step (shape will be [batch * time-steps, out_channels]).
#     # In the case of a bidirectional RNN, we want to take the last time-step of the forward RNN, 
#     # and the first time-step of the backward RNN. 
#     if not bidirectional:
#         outputs = outputs[-1]
#     else:
#         half = int(outputs[0].shape.as_list()[-1] / 2)
#         outputs = tf.concat([outputs[-1][:,:half], 
#                            outputs[0][:,half:]], 
#                           axis=1)

#     # Expand the batch * time-steps back (shape will be [batch_size, time_steps, out_channels]
#     if bidirectional:
#         out_channels = 2 * out_channels
#     outputs = tf.reshape(outputs, [batch_size, time_steps_after_stride, out_channels])
    
#     return outputs


gpu = tf.config.experimental.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(gpu[0], True)
simpleConv = SimpleConv()
def get_temp_conv_entropy(input_file):
    cap = cv2.VideoCapture(input_path)
    frame_count=0
    frame_sequence = []
    
    total_entropy = 0
    while True:
        ret, frame = cap.read()
        if ret is False:
            break

        frame = tf.image.resize(frame, [512,512], method='bilinear')/255.0
        if frame_count%24==0:
            if frame_count == 5:
                frame_sequence_tensor = np.stack(frame_sequence, axis=0)
                temp_conv_feature = simpleConv(frame_sequence_tensor)

                for channel in range(4):
                    signal = tf.keras.backend.flatten(temp_conv_feature[:,:,:,channel]).numpy().round(decimals=3)
                    total_entropy += conv_entropy(signal)

                frame_count = 0; temp_conv_feature=[]
            else:
                frame_sequence.append(frame) 

            

        frame_count += 1
# if __name__=="__main__":
    # s = time.time()
    # input_path = '/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4'
    # total_entropy = get_temp_conv_entropy(input_path)
    
