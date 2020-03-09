import subprocess as sp
import shlex

def run_command(cmd):
    p = sp.run(
        shlex.split(cmd),
        stdout=sp.PIPE, stderr=sp.PIPE,
        encoding='utf-8',
    )
    if len(p.stderr)>0 and p.stderr is not None:
        print(p.stderr)
    return p.stdout, p.stderr
