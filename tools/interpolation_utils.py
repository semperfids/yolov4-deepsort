## Libraries ###########################################################
import numpy as np
import os, argparse
import pandas as pd
from datetime import timedelta as td

## Paths and global variables ##########################################
programmer = False

## Class/Functions #####################################################
class interpolation:
    ''' Constructor '''
    def __init__(self, input_csv, output_csv = None, window_time = 10.0, max_overlap = 0.7):
        self.input_csv = input_csv
        if output_csv is None:
            odir, ofile = os.path.split(input_csv)
            ofile, ext = os.path.splitext(ofile)
            self.output_csv = os.path.join(odir, ofile + "_ouput" + ext)
        else: self.output_csv = output_csv
        self.window_time = window_time
        self.max_overlap = max_overlap
    
    ''' User functions '''
    def read_csv(self, input_csv = None):
        if input_csv is None: df = pd.read_csv(self.input_csv, header=0)
        else: df = pd.read_csv(input_csv, header=0)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    
    def frames_interpolation(self, frames):
        types = frames.dtypes
        frames = frames.set_index("frame").sort_index()
        result = pd.DataFrame(columns = frames.columns) # Empty results
        for track_id in frames["tracking_id"].unique():
            id_tab = frames[frames["tracking_id"] == track_id]
            start, end = id_tab.index.values[0], id_tab.index.values[-1] # Find empty frames and fill in
            empty_frames = sorted(set(range(start, end + 1)).difference(id_tab.index.values))
            if len(empty_frames) > 0: # For empty frames
                id_tab = id_tab.reindex(id_tab.index.to_list() + empty_frames).sort_index()
            result = result.append(id_tab.ffill())
        result = result.sort_index().reset_index().rename(columns = {"level_0":"frame"})
        result["index"] = result.index.values + 1 # fix index col
        result = result.astype(types)
        return result

    def id_corrections(self, frames):
        new_detections = frames.set_index("frame").copy()
        wt = td(seconds=self.window_time)
        for t in np.arange(frames["timestamp"].min(), frames["timestamp"].max(), wt):
            t = pd.to_datetime(t)
            idx_replace = self.__non_max_suppression(new_detections[(new_detections["timestamp"] > t) & (new_detections["timestamp"] <= t + wt)])
            if programmer: print(idx_replace)
            if len(idx_replace) > 0:
                for rep in idx_replace:
                    key,value = list(rep.keys())[0], list(rep.values())[0]
                    proof_tab = new_detections[(new_detections["tracking_id"] == key) | (new_detections["tracking_id"] == value)]
                    if not (proof_tab.reset_index().groupby("frame").size().unique() != [1]).any():
                        new_detections.replace({"tracking_id": rep}, inplace=True)
        return new_detections.reset_index()

    def to_csv(self, df, output_csv = None):
        if output_csv is None: df.to_csv(self.output_csv, index = False)
        else: df.to_csv(output_csv, index = False)
    
    ''' Hidden functions '''
    # IoU with over all values with respect to the second argument
    def __intersection_over_union(self, box1, box2): 
        # Find the intersection coordinates and then, the intersection area
        if (box1["tracking_id"] != box2["tracking_id"]) and (box1.name == box2.name): # Different boxes in same frame
            return 0.0
        else:
            x1, y1 = max(box1["x_min"], box2["x_min"]), max(box1["y_min"], box2["y_min"])
            x2, y2 = min(box1["x_max"], box2["x_max"]), min(box1["y_max"], box2["y_max"])
            interArea = max(0, x2 - x1) * max(0, y2 - y1)

            # Compute union area as: U = A1 + A2 - I
            unionArea = float((box1["y_max"] - box1["y_min"])*(box1["x_max"] - box1["x_min"]))\
                + float((box2["y_max"] - box2["y_min"])*(box2["x_max"] - box2["x_min"])) - interArea
            return interArea / unionArea # IoU

    def __non_max_suppression(self, df):
        df = df.copy()
        new_result = [] # Save the results
        while len(df) > 0:
            iou_table = df[df.apply(self.__intersection_over_union, args = ([df.iloc[0]]), axis = 1) > self.max_overlap] # IoU estimate
            for id in iou_table["tracking_id"].unique():
                if id != iou_table["tracking_id"].min():
                    # print(iou_table[(iou_table["tracking_id"] == id) | (iou_table["tracking_id"] == iou_table["tracking_id"].min())])
                    new_result.append({id: iou_table["tracking_id"].min()}) # Add only the best result
            df.drop(iou_table.index, inplace = True) # Remove old results
        return new_result

## Main ################################################################
def main(args):
    inter = interpolation(args.input_csv, args.output_csv, args.window_time, args.max_overlap)
    df = inter.read_csv()
    df = inter.id_corrections(df)
    df = inter.frames_interpolation(df)
    inter.to_csv(df)
    print(df)

def parse_args():
    '''parse args'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, 
        description='Improve object_tracker.py results with time-filter and interpolation techniques.',
        epilog='''Example:\n\t python %(prog)s -icsv test.csv # Read csv files and handled it''')
    parser.add_argument('-icsv', '--input_csv', required = True, type=str, help='Input csv path file to process')
    parser.add_argument('-ocsv', '--output_csv', default = None, type=str, help='Output csv path file to save results (default: Same that input_csv + "_output" label)')
    parser.add_argument('-wt', '--window_time', default = 10.0, type=float, help='Window time (default: %(default)s)')
    parser.add_argument('-mo', '--max_overlap', default = 0.7, type=float, help='Max overlaping in IoU metric (default: %(default)s)')
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())

# source_video,store_id,camera_id,index,tracking_id,frame,timestamp,confidence,x_min,y_min,x_max,y_max