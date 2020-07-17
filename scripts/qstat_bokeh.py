#!/usr/bin/env python3
import os
import yaml
import argparse
from itertools import cycle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from qstats import qstats

from bokeh.io import save, output_file
from bokeh.plotting import figure
from bokeh.models import HoverTool, Panel, Tabs, DataRange1d
from bokeh.layouts import layout, Column
from bokeh.transform import cumsum

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--filename",
        default="index.html",
        help="Output html file to save the bokeh plots",
    )
    return parser.parse_args()

def get_queue_status():
    queue_status = qstats.queue_status()
    queue_status["queue"] = queue_status["name"]
    queue_status["njobs"] = queue_status["available"]
    queue_status["owner"] = "free"
    queue_status["name"] = "Free"
    queue_status["color"] = "#d9d9d9"
    return queue_status

def get_pending_jobs():
    pending_jobs = qstats.pending_jobs(columns=[
        "@state", "JB_job_number", "JB_owner",
        "hard_req_queue", "slots", "tasks"
    ])

    # conversions
    pending_jobs["queue"] = pending_jobs["hard_req_queue"].str.split("@").str[0]
    pending_jobs["njobs"] = pending_jobs["slots"]
    pending_jobs["owner"] = pending_jobs["JB_owner"]

    color = cycle([
        "#80b1d3", "#fb8072", "#fdb462", "#b3de69", "#8dd3c7", "#ffffb3",
        "#bebada", "#fccde5", "#bc80bd", "#ccebc5", "#ffed6f",
    ])
    pending_jobs["color"] = pending_jobs["owner"].map({
        k: next(color) for k in pending_jobs["owner"].unique()
    })

    users_dict = {}
    if os.path.exists(userfile := os.path.expanduser("~/.users.yaml")):
        with open(userfile, 'r') as f:
            users_dict = yaml.safe_load(f)
    pending_jobs["name"] = pending_jobs["owner"].map(users_dict)

    return pending_jobs

def get_njobs():
    pending_jobs = get_pending_jobs()
    queue_status = get_queue_status()

    nrunning_jobs = (
        pending_jobs.loc[pending_jobs["@state"]=='running']
        .groupby(["queue", "owner", "name", "color"], as_index=False)[["njobs"]]
        .sum()
    )
    nrunning_jobs = (
        pd.concat([
            nrunning_jobs,
            queue_status[["queue", "owner", "name", "color", "njobs"]],
        ]).astype({"njobs": int})
        .sort_values(["queue", "njobs"])
    )

    def pie_order(n):
        x = np.arange(1, n+1)
        result = np.arange(1, n+1)

        if n%2 == 0:
            result[2::2] = x[1:(x.shape[0]+1)//2]
            result[1::2] = x[(x.shape[0]+1)//2:]
        else:
            result[2::2] = x[(x.shape[0]+1)//2:]
            result[1::2] = np.roll(x[1:(x.shape[0]+1)//2], 1)
        result[1:-1] = result[-2:0:-1]
        return result

    temp = pd.DataFrame()
    for cat, sub in nrunning_jobs.groupby("queue"):
        sub.index = pie_order(sub.shape[0])
        temp = pd.concat([temp, sub.sort_index()])
    nrunning_jobs = temp.set_index("queue")

    nrunning_jobs["frac"] = (
        nrunning_jobs[["njobs"]] /
        nrunning_jobs.groupby("queue")[["njobs"]].sum()
    )
    nrunning_jobs["prct"] = 100*nrunning_jobs["frac"]
    nrunning_jobs["angle"] = nrunning_jobs["frac"]*2*np.pi
    nrunning_jobs = nrunning_jobs.reset_index()

    npending_jobs = (
        pending_jobs.loc[pending_jobs["@state"]=='pending']
        .groupby(["queue", "owner", "name", "color"], as_index=False)[["njobs"]]
        .sum()
        .sort_values(["queue", "njobs"], ascending=False)
        .reset_index(drop=True)
    )

    return nrunning_jobs, npending_jobs

def plot_pie(data, title='Running'):
    p = figure(
        plot_height=500, plot_width=500, title=title,
        x_range=(-1.25, 1.25), y_range=(-1.25, 1.25),
        tools="pan,wheel_zoom,save,reset",
    )

    wedge = p.wedge(
        x=0, y=0, radius=0.8,
        source=data,
        start_angle=cumsum('angle', include_zero=True),
        end_angle=cumsum('angle'),
        line_color='color', fill_color='color',
    )

    p.add_tools(HoverTool(
        renderers=[wedge],
        tooltips=[('Name', '@name'), ('Used', '@njobs')]
    ))

    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None

    angles = np.convolve(data["angle"].cumsum(),  [0.5, 0.5], mode='same')
    if data.shape[0]>1:
        selection = (np.pi/2<angles) & (angles<3*np.pi/2)

        if (~selection).any():
            p.text(
                0.82*np.cos(angles[~selection]), 0.82*np.sin(angles[~selection]),
                text=data.loc[~selection,"name"], angle=angles[~selection],
                text_font_size='12px', text_align='left', text_baseline='middle',
            )
            p.text(
                0.4*np.cos(angles[~selection]), 0.4*np.sin(angles[~selection]),
                text=[f'{x:.1f}%' for x in data.loc[~selection,"prct"]],
                angle=angles[~selection],
                text_font_size='12px', text_align='left', text_baseline='middle',
            )

        if selection.any():
            p.text(
                0.82*np.cos(angles[selection]), 0.82*np.sin(angles[selection]),
                text=data.loc[selection,"name"], angle=angles[selection]-np.pi,
                text_font_size='12px', text_align='right', text_baseline='middle',
            )
            p.text(
                0.4*np.cos(angles[selection]), 0.4*np.sin(angles[selection]),
                text=[f'{x:.1f}%' for x in data.loc[selection,"prct"]],
                angle=angles[selection]-np.pi,
                text_font_size='12px', text_align='right', text_baseline='middle',
            )
    else:
        p.text(
            [0.82], [0], text=data["name"],
            text_font_size='12px', text_align='left', text_baseline='middle',
        )
        p.text(
            [0.4], [0], text=[f'{x:.1f}%' for x in data["prct"]],
            text_font_size='12px', text_align='left', text_baseline='middle',
        )
    return p

def plot_bar(data, title="Queued"):
    p = figure(
        plot_height=500, plot_width=500, title=title,
        x_range=data["name"], y_range=DataRange1d(start=0),
        tools="ypan,ywheel_zoom,save,reset,hover",
        tooltips=[('Name', '@name'), ('Queued', '@njobs')]
    )
    p.vbar(
        x='name', top='njobs', width=0.9, source=data,
        line_color='white', fill_color='color',
    )
    return p

def generate_bokeh_html(running, pending, filename="index.html"):
    output_file(filename, title="Jobs")
    tabs = []
    for queue in ["hep.q", "gpu.q", "fw.q"]:
        l = layout([[
            plot_pie(
                running.query("queue==@queue and njobs>0"),
                title="Running",
            ),
            plot_bar(
                pending.query("queue==@queue and njobs>0"),
                title="Queued",
            ),
        ]])
        tabs.append(Panel(child=l, title=queue))
    save(Tabs(tabs=tabs))

def main(filename):
    generate_bokeh_html(*get_njobs(), filename=filename)

if __name__ == "__main__":
    main(**vars(parse_args()))
