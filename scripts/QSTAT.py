import argparse
import getpass
import datetime
import numpy as np
import pandas as pd
import tabulate as tab

from qstats import queue_status, pending_jobs
from qstats.utils import print_frame

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u", "--users", default=getpass.getuser(),
        help="Select user(s), comma-delimited",
    )
    #parser.add_argument("--usernames", default=False, action="store_true",
    #                    help="Print usernames instead of names")
    parser.add_argument(
        "-q", "--queue", default="hep.q,gpu.q,fw.q",
        help="Select queue(s), comma-delimited, e.g. 'hep.q,gpu.q'",
    )
    #parser.add_argument("-l", "--loglevel", default="INFO",
    #                    help="Logging level (DEBUG, INFO, WARNING, ERROR or CRITICAL)")
    parser.add_argument(
        "-f", "--format", default="presto",
        help="Tabulate format (https://bitbucket.org/astanin/python-tabulate)",
    )
    #parser.add_argument(
    #    "-c", "--cols",
    #    default="running,sum:pending,sum:duration,min:duration,mean:duration,max:duration,sum:priority,max",
    #    help="Additional column(s), comma-delimited colon-separated",
    #)
    options = parser.parse_args()
    options.users = options.users.split(",")
    options.queue = options.queue.split(",")
    #options.cols = [c.split(",") for c in options.cols.split(":")]
    return options

def queue_status_frame():
    df = queue_status()
    df = df.set_index("name")
    df.index.names = ["queue"]
    df = df.loc[:,["available", "used", "total", "unknown", "error"]]
    return df.loc[:,df.sum()>0]

def job_status_frame():
    df = pending_jobs()
    df["queue"] = df["hard_req_queue"]
    df["user"] = df["JB_owner"]
    df["state"] = df["@state"]
    df["jobs"] = df["slots"]
    df["priority"] = df["JAT_prio"]*200.
    df["duration"] = (datetime.datetime.now() - df["JAT_start_time"])
    df["duration"] = (
        df["duration"].replace({np.nan: pd.Timedelta(seconds=0)})
        .astype("int64")
    )

    df = pd.DataFrame(
        df.groupby(["queue", "user", "state"])[["jobs", "duration", "priority"]]
        .agg(["sum", "min", "mean", "max"])
    )
    df["duration"] = df["duration"].fillna(0.).astype("timedelta64[ns]")

    df = df[[
        ("jobs", "sum"), ("duration", "min"), ("duration", "mean"),
        ("duration", "max"), #("duration", "sum"),
        ("priority", "max"),
    ]].unstack()
    df.columns.names = ["label", "agg", "state"]
    df.columns = df.columns.reorder_levels(["state", "label", "agg"])
    df = df[[
        ("running", "jobs", "sum"), ("pending", "jobs", "sum"),
        ("running", "duration", "min"), ("running", "duration", "mean"),
        ("running", "duration", "max"), #("running", "duration", "sum"),
        ("pending", "priority", "max"),
    ]]
    df.columns = pd.MultiIndex.from_tuples([
        ("running", "sum"), ("pending", "sum"),
        ("duration", "min"), ("duration", "mean"), ("duration", "max"),
        #("duration", "sum"),
        ("priority", "max"),
    ])
    df[("running", "sum")] = df[("running", "sum")].fillna(0).astype("int64")
    df[("pending", "sum")] = df[("pending", "sum")].fillna(0).astype("int64")
    df[("duration", "min")] = df[("duration", "min")].dt.total_seconds().astype("timedelta64[s]")
    df[("duration", "mean")] = df[("duration", "mean")].dt.total_seconds().astype("timedelta64[s]")
    df[("duration", "max")] = df[("duration", "max")].dt.total_seconds().astype("timedelta64[s]")
    #df[("duration", "sum")] = df[("duration", "sum")].dt.total_seconds().astype("timedelta64[s]")
    df[("priority", "max")] = df[("priority", "max")]
    df = df.reorder_levels(["user", "queue"]).unstack()
    df.columns.names = ["label", "agg", "queue"]
    df.columns = df.columns.reorder_levels(["queue", "label", "agg"])
    df.columns.names = [None]*3
    return df

def main(users, queue, format):
    dfq = queue_status_frame()
    dfj = job_status_frame()

    dfq = dfq.query("queue in @queue").sort_values("used")
    dfj = dfj[[q for q in queue if q in (c[0] for c in dfj.columns)]]
    dfj = dfj.sort_values((queue[0], "running", "sum"), ascending=False)

    dfj.columns = ["\n".join(c) for c in dfj.columns]

    lines = tab.tabulate(dfq, headers='keys', tablefmt=format).splitlines()
    lines[0] = "|".join(['\033[1m{}\033[0m'.format(c) for c in lines[0].split("|")])
    print("")
    print("\n".join(lines))
    print("")

    repeats = []
    lines = tab.tabulate(dfj, headers='keys', tablefmt=format, floatfmt='.3f').splitlines()
    new_lines = []
    for il, line in enumerate(lines):
        new_line = []
        if il < 2:
            for c in line.split("|"):
                if c.strip() in repeats:
                    c = " "*len(c)
                else:
                    repeats.append(c.strip())
                new_line.append('\033[1m{}\033[0m'.format(c))
        elif il == 2:
            new_line = ['\033[1m{}\033[0m'.format(c) for c in line.split('|')]
        else:
            if line.split("|")[0].strip() in users:
                new_line = '\033[95m\033[1m{}\033[0m'.format(line).split("|")
            else:
                new_line = line.split("|")
        new_lines.append("|".join(new_line))
    print(
        "\n".join(new_lines)
        .replace("nan", "  -")
        .replace("0 days 00:00:00", "-"+" "*14)
    )
    print("")

    #print_frame(dfq, users=users, tablefmt=format)
    #print("")
    #print_frame(
    #    dfj, users=users, multiheader=True, tablefmt=format, floatfmt=".4f",
    #)

if __name__ == "__main__":
    main(**vars(parse_args()))
