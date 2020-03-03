import subprocess as sp
import shlex
import tabulate as tab
import numpy as np

def run_command(cmd):
    p = sp.run(
        shlex.split(cmd),
        stdout=sp.PIPE, stderr=sp.PIPE,
        encoding='utf-8',
    )
    if len(p.stderr)>0 and p.stderr is not None:
        print(p.stderr)
    return p.stdout, p.stderr

class tfmt:
    DARKCYAN  = '\033[36m'
    RED       = '\033[91m'
    GREEN     = '\033[92m'
    YELLOW    = '\033[93m'
    BLUE      = '\033[94m'
    MAGENTA   = '\033[95m'
    CYAN      = '\033[96m'
    BRED      = '\033[101m'
    BGREEN    = '\033[102m'
    BYELLOW   = '\033[103m'
    BBLUE     = '\033[104m'
    BMAGENTA  = '\033[105m'
    BCYAN     = '\033[106m'
    BOLD      = '\033[1m'
    DIM       = '\033[2m'
    UNDERLINE = '\033[4m'
    BLINK     = '\033[5m'
    END       = '\033[0m'

def print_frame(df, users=None, multiheader=False, **kwargs):
    if not multiheader:
        headers = [tfmt.BOLD+c+tfmt.END for c in df.columns]
    else:
        headers = []
        repeats = []
        for hs in zip(*df.columns.to_frame().values.T):
            line = []
            for i, h in enumerate(hs):
                if h in repeats:
                    line.append('')
                else:
                    if i==0:
                        repeats.append(h)
                    line.append(tfmt.BOLD+h+tfmt.END)
                print(repeats)

            headers.append("\n".join(line))
    to_print = tab.tabulate(df.values, headers=headers, **kwargs)
    if users:
        new_to_print = []
        for line in to_print.split("\n"):
            if any(user in line for user in users):
                new_to_print.append(tfmt.MAGENTA+tfmt.BOLD+line+tfmt.END)
            else:
                new_to_print.append(line)
        to_print = "\n".join(new_to_print)
    print(to_print.replace("nan", "  -"))
