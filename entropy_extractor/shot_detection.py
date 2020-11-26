class ShotDetector():
    def __init__(self, threshold=12, min_percent=0.95, min_scene_len=2, block_size=8):
        """Initializes threshold-based scene detector object."""
        self.threshold = int(threshold)
        self.min_percent = min_percent
        self.min_scene_len = min_scene_len
        self.processed_frame = False
        self.last_cut = 0
        self.block_size = block_size
        self.shot_list = [] # frame number where the last detected fade is, type of fade, can be either 1: 'over' or 0: 'below'
        self.frame_num = 0
    def frame_under_threshold(self, frame):
        num_pixel_values = float(frame.shape[0] * frame.shape[1] * frame.shape[2])
        large_ratio = self.min_percent > 0.5
        ratio = 1.0 - self.min_percent if large_ratio else self.min_percent
        min_pixels = int(num_pixel_values * ratio)

        curr_frame_amt = 0
        curr_frame_row = 0

        while curr_frame_row < frame.shape[0]:

            block = frame[curr_frame_row : curr_frame_row + self.block_size, :, :]
            if large_ratio:
                curr_frame_amt += int(numpy.sum(block > self.threshold))
            else:
                curr_frame_amt += int(numpy.sum(block <= self.threshold))

            if curr_frame_amt > min_pixels:
                return not large_ratio
            curr_frame_row += self.block_size
        return large_ratio

    def process_frame(self, frame_img):
        print(self.frame_num, frame_img)
        under_th = self.frame_under_threshold(frame_img) % 2
        if self.processed_frame:
            if under_th ^ self.shot_list[-1][0]:
                if(self.frame_num - self.last_cut)>=self.min_scene_len:
                    self.shot_list.append([ under_th % 2, self.frame_num-1])
                    self.last_cut = self.frame_num
                elif len(self.shot_list)==1:
                    self.shot_list[0][0] = (self.shot_list[0][0] + 1) % 2
                else:
                    self.shot_list.pop()
                    self.last_cut = self.shot_list[-1][1]
        else:           
            self.shot_list.append([under_th, 0])
            self.last_cut = 0
            
        print(self.shot_list)
        self.processed_frame = True
        self.frame_num += 1

    def post_process(self):
        if self.frame_num - self.last_cut <= self.min_scene_len:
            self.shot_list[-1][1] = self.frame_num-1
        else:
            self.shot_list.append([ (self.shot_list[-1][0]+1)%2, self.frame_num-1] )
        if len(self.shot_list)>1:
            del self.shot_list[0]

if __name__=="__main__":
    shotDetector = ShotDetector()
    video = [0,0,0,0,1,0,1,1,1,0,0]
    for n, v in enumerate(video):
        shotDetector.process_frame(v)
    shotDetector.post_process()
    print(shotDetector.shot_list)
    
