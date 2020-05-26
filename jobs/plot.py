import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def main():
    plot_visit_per_hour("20160803")
    return None

def read(func, date):
    from os import listdir
    from os.path import isfile, join
    directory = os.getcwd()+ "/out/" + func + date + "/"
    file_list = listdir(directory)
    file_list = [f for f in file_list if f.endswith(".csv")]
    selected_file_paths = [directory + f for f in file_list]
    df = pd.read_csv(selected_file_paths[0])
    return df

def plot_visit_per_hour(date):
    df = read("visit_per_hour", date)
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d%H', errors='coerce')
    df.plot(x ='time', y='visits', kind = 'line')
    directory = os.getcwd()+ "/fig/" + "visit_per_hour" + date
    plt.savefig(directory)
    return None


if __name__ == "__main__":
    main()