import numpy as np
import pandas as pd
import xmltodict

from .utils import run_command

__all__ = [
    "pending_jobs",
    "finished_jobs",
    "all_jobs",
    "queue_status",
]

all_columns = [
    "@state", "JB_job_number", "JAT_prio", "JB_name", "JB_owner", "state",
    "JB_submission_time", "JAT_start_time", "JAT_end_time", "cpu_usage",
    "mem_usage", "io_usage", "queue_name", "slots", "tasks", "full_job_name",
    "exit_status", "failed", "maxvmem", "hard_req_queue",
]

queue_columns = [
    "name", "used", "available", "total", "unknown", "error",
]

def pending_jobs(columns=all_columns):
    out, err = run_command('qstat -xml -ext -r -urg -g dt -u "*"')
    out_dict = xmltodict.parse(out)
    df_run = pd.DataFrame(out_dict["job_info"]["queue_info"]["job_list"])

    none_pending = out_dict["job_info"]["job_info"] is None
    if not none_pending:
        df_pen = pd.DataFrame(out_dict["job_info"]["job_info"]["job_list"])
    else:
        df_pen = pd.DataFrame()

    # fill missing values
    df_run["JB_submission_time"] = np.nan
    df_run["JAT_end_time"] = np.nan
    df_run["exit_status"] = 0
    df_run["failed"] = 0
    df_run["maxvmem"] = np.nan

    df_pen["JAT_start_time"] = np.nan
    df_pen["JAT_end_time"] = np.nan
    df_pen["cpu_usage"] = 0.
    df_pen["mem_usage"] = 0.
    df_pen["io_usage"] = 0.
    df_pen["exit_status"] = 0
    df_pen["failed"] = 0
    df_pen["maxvmem"] = np.nan

    if "tasks" not in df_run.columns:
        df_run["tasks"] = np.nan
    if "tasks" not in df_pen.columns:
        df_pen["tasks"] = np.nan
    df_run["tasks"] = df_run["tasks"].fillna(0)
    df_pen["tasks"] = df_pen["tasks"].fillna(0)

    # match columns and merge
    df_run = df_run.loc[:, columns]
    if not none_pending:
        df_pen = df_pen.loc[:, columns]
        df = pd.concat([df_run, df_pen], axis='index')
    else:
        df = df_run.copy()

    df = df.astype({
        "@state": "category",
        "JB_job_number": "uint64",
        "JAT_prio": "float64",
        "state": "category",
        "JB_submission_time": "datetime64[s]",
        "JAT_start_time": "datetime64[s]",
        "JAT_end_time": "datetime64[s]",
        "cpu_usage": "float64",
        "mem_usage": "float64",
        "io_usage": "float64",
        "slots": "uint64",
        "tasks": "uint64",
        "exit_status": "uint64",
        "failed": "uint64",
        "maxvmem": "float64",
    })

    return (
        df.sort_values(["JB_job_number", "tasks"])
        .reset_index(drop=True)
    )

def finished_jobs(path="/opt/sge/default/common/accounting", columns=all_columns):
    df = pd.read_csv(path, sep=':', header=None, usecols=list(range(45)))
    df.columns = [
        "qname", "hostname", "group", "JB_owner", "JB_name", "JB_job_number",
        "account", "JAT_prio", "JB_submission_time", "JAT_start_time",
        "JAT_end_time", "failed", "exit_status", "ru_wallclock", "ru_utime",
        "ru_stime", "ru_maxrss", "ru_ixrss", "ru_ismrss", "ru_idrss",
        "ru_isrss", "ru_minflt", "ru_majflt", "ru_nswap", "ru_inblock",
        "ru_oublock", "ru_msgsnd", "ru_msgrcv", "ru_nsignals", "ru_nvcsw",
        "ru_nivcsw", "project", "department", "granted_pe", "slots", "tasks",
        "cpu_usage", "mem_usage", "io_usage", "category", "iow", "pe_taskid",
        "maxvmem", "arid", "ar_submission_time",
    ]

    # add state columns for finished/failed
    df["state"] = "f" # finished
    df["@state"] = "finished"
    mask = df.eval("failed != '0' or exit_status != '0'")
    df.loc[mask,"state"] = "F" # failed
    df.loc[mask,"@state"] = "failed"

    df["hard_req_queue"] = df["qname"]
    df["queue_name"] = df["qname"]+"@"+df["hostname"]
    df["full_job_name"] = df["JB_name"]
    df = df.loc[:,columns]

    df = df.astype({
        "@state": "category",
        "JB_job_number": "uint64",
        "JAT_prio": "float64",
        "state": "category",
        "JB_submission_time": "datetime64[s]",
        "JAT_start_time": "datetime64[s]",
        "JAT_end_time": "datetime64[s]",
        "cpu_usage": "float64",
        "mem_usage": "float64",
        "io_usage": "float64",
        "slots": "uint64",
        "tasks": "uint64",
        "exit_status": "uint64",
        "failed": "uint64",
        "maxvmem": "float64",
    })
    return df

def all_jobs(path="/opt/sge/default/common/accounting", columns=all_columns):
    df_pen = pending_jobs(columns=columns)
    df_fin = finished_jobs(path, columns=columns)
    return pd.concat([df_pen, df_fin], axis='index').reset_index(drop=True)

def queue_status(columns=queue_columns):
    out, err = run_command('qstat -xml -ext -r -urg -g c')

    out_dict = xmltodict.parse(out)
    queues = out_dict["job_info"]["cluster_queue_summary"]

    return pd.DataFrame(queues).loc[:,columns]
