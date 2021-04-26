import os 
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context
class Decision():
    def __init__(self, clip_name='None', a_type='None', d_type='None', a_parameter=-1, prev_fps=24, prev_bitrate=1000, fps=24, bitrate=1000, sample_quality_ratio=1, shot_list=[], storage_dir=""):

        self.clip_name = clip_name
        self.a_type = a_type
        self.d_type = d_type
        self.a_param = a_parameter
        self.fps = fps
        self.bitrate = bitrate
        self.shot_list = shot_list
        self.day_idx, self.time_idx = get_context(clip_name)
        self.prev_fps = prev_fps 
        self.prev_bitrate = prev_bitrate
        self.sample_quality_ratio = sample_quality_ratio
        self.storage_dir = os.path.join(storage_dir, clip_name.split('_')[-2]+"_00-00-00")
        