from datetime import datetime

def compute_diff(time):
    start = datetime.strptime(time[0], "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(time[-1], "%Y-%m-%d %H:%M:%S")
    return str(int((end - start).seconds / 60)) + " MIN"