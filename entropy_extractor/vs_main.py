from color import get_color_entropy
from edge import get_edge_entropy
from conv import get_conv_entropy
from temporal import get_temp_conv_entropy
from multiprocessing import Process, Value
from influxdb import InfluxDBClient

DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')

def get_visual_feature(input_path):
    color_entropy = Value('d', 0.0); edge_entropy = Value('d', 0.0); conv_entropy = Value('d', 0.0); temp_entropy = Value('d', 0.0)
    color_proc = Process(target=get_color_entropy, args=(input_path, color_entropy,))
    edge_proc = Process(target=get_edge_entropy, args=(input_path, edge_entropy,))
    conv_proc = Process(target=get_conv_entropy, args=(input_path, conv_entropy,))
    temp_proc = Process(target=get_temp_conv_entropy, args=(input_path, temp_entropy,))
    color_proc.start(); edge_proc.start(); conv_proc.start(); temp_proc.start()
    color_proc.join(); edge_proc.join(); conv_proc.join(); temp_proc.join()


    json_body = [
                {
                    "measurement": "analy_result",
                    "tags": {
                        "name": str(S_decision.clip_name),
                        "a_type": str(S_decision.a_type),
                        "day_of_week":int(day_idx),
                        "time_of_day":int(time_idx),
                        "host": "webcamPole1"
                    },
                    "fields": {
                        "a_parameter": float(self.framesCounter),
                        "fps": float(S_decision.fps),
                        "bitrate": float(S_decision.bitrate),
                        "time_consumption": float(self.processing_time),
                        "target": int(self.target_counter)
                    }
                }
            ]


if __name__=="__main__":
    input_file = "/home/min/LiteOn_P1_2019-11-12_15:00:36.mp4"
    get_visual_feature(input_file)