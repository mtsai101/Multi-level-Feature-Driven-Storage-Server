from optimal_downsampling_manager.resource_predictor.table_estimator import get_context
class Decision():
    def __init__(self, clip_name='None', a_type='None', d_type='None', a_parameter=-1, prev_fps=24, prev_bitrate=1000, fps=24, bitrate=1000, others=[0,0,0], shot_list=[]):

        self.clip_name = clip_name
        self.a_type = a_type
        self.d_type = d_type
        self.a_param = a_parameter
        self.fps = fps
        self.bitrate = bitrate
        ## Here we use others to pass [a_param_ill, a_param_peo, raw_size]
        self.others = others
        self.shot_list = shot_list
        date =  clip_name.split('/')[-2].split('_')[0].split('-')
        self.month, self.day = int(date[1]), int(date[2])
        self.day_idx, self.time_idx = get_context(clip_name)
        self.prev_fps = prev_fps 
        self.prev_bitrate = prev_bitrate 
        