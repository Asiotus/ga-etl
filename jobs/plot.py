import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def main():
    date = "20160807"
    plot_visit_per_hour(date)
    plot_visitor_per_hour(date)
    plot_hourly_visit_pattern(date)
    plot_popular_os(date)
    plot_popular_browser(date)
    plot_country_dist(date)
    plot_average_visit_duration(date)
    top_popular_page(date)
    plot_referral_path(date)

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

def plot_visitor_per_hour(date):
    df = read("visitor_per_hour", date)
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d%H', errors='coerce')
    df.plot(x ='time', y='visitors', kind = 'line')
    directory = os.getcwd()+ "/fig/" + "visitor_per_hour" + date
    plt.savefig(directory)
    plt.clf()

def plot_hourly_visit_pattern(date):
    df = read("hourly_visit_pattern", date[:-2])
    df.plot(x ='time', y='visits', kind = 'bar')
    directory = os.getcwd()+ "/fig/" + "hourly_visit_pattern" + date[:-2]
    plt.savefig(directory)
    plt.clf()

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

def plot_country_dist(date):
    df = read("country_dist", date[:-2])
    pivot_countryPd = df.pivot(index='country', columns='time', values='count')
    pivot_countryPd.plot.barh(stacked=True, figsize=(7,25))
    directory = os.getcwd()+ "/fig/" + "country_dist" + date[:-2]
    plt.savefig(directory)
    plt.clf()

def plot_average_visit_duration(date):
    df = read("average_visit_duration", date[:-2])
    df['time'] = pd.to_datetime(df['time'], format='%Y%m%d', errors='coerce')
    df.plot(x ='time', y='duration', kind = 'line')
    directory = os.getcwd()+ "/fig/" + "average_visit_duration" + date[:-2]
    plt.savefig(directory)
    plt.clf()

def top_popular_page(date):
    hitPagePd = read("popular_page", date[:-2])
    hitPagePd['date'] = pd.to_datetime(hitPagePd['date'], format='%Y%m%d', errors='coerce')
    dateList = hitPagePd['date'].unique()
    dateList = np.sort(dateList)
    pageDailyRank = pd.DataFrame()
    rank = [i+1 for i in range(10)]
    for d in dateList:
        top10 = hitPagePd[hitPagePd['date'] == d]
        top10 = top10.sort_values(by='count', ascending=False)
        top10 = top10[:10].reset_index(drop=True)
        top10['rank'] = rank
        pageDailyRank = pageDailyRank.append(top10)
    directory = os.getcwd()+ "/fig/" + "top_popular_page" + date[:-2] + ".csv"
    pageDailyRank.to_csv(directory)

def plot_referral_path(date):
    import json
    filepath = 'out/referral_path' + date + ".json"
    with open (filepath, 'r') as fin:
        referraljson = json.load(fin)
 
    import pyecharts.options as opts
    from pyecharts.charts import Sankey
    plottitle = "top_referral_pages" + date
    plotpath = "fig/top_referral_pages" + date + ".html"
    Sankey() \
    .add(
        "sankey",
        nodes=referraljson["nodes"],
        links=referraljson["links"],
        linestyle_opt=opts.LineStyleOpts(opacity=0.2, curve=0.5, color="source"),
        label_opts=opts.LabelOpts(position="right"),
    ) \
    .set_global_opts(title_opts=opts.TitleOpts(title=plottitle)) \
    .render(plotpath)

if __name__ == "__main__":
    main()