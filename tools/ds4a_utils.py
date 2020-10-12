import csv
import datetime as dt

def get_video_name_data(video_path):
    #Ejemplo Hermeco Oficinas_10.50.60.47_2_20201002191740_20201002191804_1602193437830.mp4
    #split por underscore
    #0 -> Hermeco Oficinas
    #1 -> Un numero que ni idea pero identifica la tienda
    #2 -> Numero de cámara
    #3 -> Estampa de tiempo de inicio
    #4 -> Estampa de tiempo de fin
    #5 -> Otro id que ni idea

    video_name = video_path.split('/')[-1]

    items = video_name.split('_')

    date_time_str = items[3]
    date_time_obj = dt.datetime.strptime(date_time_str, '%Y%m%d%H%M%S')

    video_name_dict = {
        "video_name": video_name,
        "store_id": items[1],
        "camera_id": items[2],
        "init_timestamp": date_time_obj,
        "stop_timestamp": items[4]
    }
    return video_name_dict

def data_to_dict(video_name, store_id, camera_id, init_timestamp, index, tracking_id, frame, confidence, xmin, ymin, xmax, ymax):
    curr_time = compute_timestamp_from_frame(init_timestamp, frame)
    res_dict = {
        'source_video': video_name,
        'store_id': store_id,
        'camera_id': camera_id,
        'index': index,
        'tracking_id': tracking_id,
        'frame': frame,
        'timestamp': curr_time,
        'confidence': confidence,
        'x_min': xmin,
        'y_min': ymin,
        'x_max': xmax,
        'y_max': ymax
    }
    return res_dict

def write_to_csv(data, outfile):
    with open(outfile, mode='w') as csv_file:
        fieldnames = ['source_video', 'store_id', 'camera_id', 'index', 'tracking_id', 'frame', 'timestamp', 'confidence', 'x_min', 'y_min', 'x_max', 'y_max']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow(row)
    

def compute_timestamp_from_frame(init_timestamp, frame):
    const_frames_per_second = 30
    num_seconds = int(frame / const_frames_per_second)
    #Add the number of seconds to the timestamp
    return init_timestamp + dt.timedelta(0,num_seconds)

