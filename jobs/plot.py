import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def main():
    plot_visit_per_hour("20160803")
    plot_visitor_per_hour("20160803")
    plot_hourly_visit_pattern("20160803")
    plot_popular_os("20160803")
    plot_popular_browser("20160803")
    plot_country_dist("20160803")
    average_visit_duration("20160803")
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
    plt.clf()
    return None

def plot_visitor_per_hour(date):
    df = read("visitor_per_hour", date)
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d%H', errors='coerce')
    df.plot(x ='time', y='visitors', kind = 'line')
    directory = os.getcwd()+ "/fig/" + "visitor_per_hour" + date
    plt.savefig(directory)
    plt.clf()
    return None

def plot_hourly_visit_pattern(date):
    df = read("hourly_visit_pattern", date[:-2])
    df.plot(x ='time', y='visits', kind = 'bar')
    directory = os.getcwd()+ "/fig/" + "hourly_visit_pattern" + date[:-2]
    plt.savefig(directory)
    plt.clf()
    return None

def plot_popular_os(date):
    df = read("popular_os", date[:-2])
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d', errors='coerce')
    OSList = df['os'].unique()
    fig = plt.figure(figsize=(10,5))
    ax = plt.subplot(111)
    for o in OSList:
        n = df[df['os'] == o]
        ax.plot('time','count',data = n, label = o)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    directory = os.getcwd()+ "/fig/" + "popular_os" + date[:-2]
    plt.savefig(directory)
    plt.clf()
    return None

def plot_popular_browser(date):
    df = read("popular_browser", date[:-2])
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d', errors='coerce')
    browserList = df['browser'].unique()
    fig = plt.figure(figsize=(10,5))
    ax = plt.subplot(111)
    for b in browserList:
        n = df[df['browser'] == b]
        ax.plot('time','count',data = n, label = b)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    directory = os.getcwd()+ "/fig/" + "popular_browser" + date[:-2]
    plt.savefig(directory)
    plt.clf()
    return None

def plot_country_dist(date):
    df = read("country_dist", date[:-2])
    pivot_countryPd = df.pivot(index='country', columns='time', values='count')
    pivot_countryPd.plot.barh(stacked=True, figsize=(7,25))
    directory = os.getcwd()+ "/fig/" + "country_dist" + date[:-2]
    plt.savefig(directory)
    plt.clf()
    return None

def average_visit_duration(date):
    df = read("average_visit_duration", date[:-2])
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d', errors='coerce')
    df.plot(x ='time', y='duration', kind = 'line')
    directory = os.getcwd()+ "/fig/" + "average_visit_duration" + date[:-2]
    plt.savefig(directory)
    plt.clf()
    return None

if __name__ == "__main__":
    main()