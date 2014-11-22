#!/usr/bin/env python

import sys
from suprocess import call
import argparse
import datetime
import pickle


SUSPENDED = 'suspended'
KILLED = 'killed'
RUNNING = 'running'
QUEUED = 'queued'
DONE = 'done'
SLEEPING = 'sleeping'
WAITING = 'waiting'

JOBS = ([], {})


def sub(jobs):
    for job in jobs:
      call('sqsub -r {} {}'.format(
          job.cpu_time_limit, job.command))


def kill(jobs):
    for job in jobs:
        call('sqkill {}'.format(job.jobid))
        job.state = KILLED


def resume(jobs):
    for job in jobs:
      call('sqresume {}'.format(job.jobid))


def jobs():
    return JOBS[0]


def suspend(jobs):
    for job in jobs:
        call('sqsuspend {}'.format(job.jobid))
        job.state = SUSPENDED

def load(s):
    with open(s, 'r') as p:
        JOBS = pickle.load(p)


def save(jobs, filename):
    with open(filename, 'w') as out:
        pickle.dump(JOBS, out)


def update():
    pass


def job(command, time='1m'):
    j = argparse.Namespace()
    j.command = command
    j.cpu_time_limit = duration(time)
    return j


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-jobfile', type=open, default=sys.stdin)
    parser.add_argument('-output', default=sys.stdout)
    return parser.parse_args()


def parse_table(lines, keytype):
    job = argparse.Namespace()
    for line in lines:
        if not line.strip():
            break
        if line.startswith('-'):
            continue
        keyend = line.find(':')
        key = line[:keyend].strip()
        value = line[keyend + 1:].strip()
        if key in keytype:
            value = keytype[key](value)
        key = key.replace(' ', '_')
        setattr(job, key, value)
    return job


def parse_state_table(lines, keynames):
    next(lines)
    details = dict()
    for line in lines:
        fields = line.split()[:3]
        state = fields[2]
        if state in keynames:
            state = keynames[state]
        pid = fields[1]
        details[int(fields[0])] = pid, state
    return details


def duration(s):
    durkey = dict(s=1,m=60,h=60*60,d=60*60*24)
    return datetime.timedelta(int(s[:-1]) * durkey[s[-1]])


def memsize(s):
    memkey = dict(K=1,M=2,G=3,T=4,P=5)
    return float(s[:-1]) * (1024 ** memkey[s[-1].upper()])


if __name__ == "__main__":
    args = parse_args()

    dateformat = '%a %b %d %H:%M:%S %Y'
    date = lambda s: datetime.datetime.strptime(
            ' '.join(s.split()[:5]), dateformat)
    arglist = lambda s: s.strip()
    path = lambda i: i
    snames = dict(R=RUNNING, Q=QUEUED, Z=SLEEPING,
            S=SUSPENDED, K=KILLED, T=SUSPENDED,
            D=DONE)
    snames['*Q'] = WAITING

    ttypes = {
        'jobid': int,
        'queue': str,
        'ncpus': int,
        'nodes': str,
        'command': arglist,
        'working directory': path,
        'out file': path,
        'out file age': duration,
        'submitted': date,
        'started': date,
        'should end': date,
        'elapsed limit': duration,
        'cpu time limit': duration,
        'virtual mem limit': memsize,
   }

    jobs = []
    byid = dict()
    lines = iter(args.jobfile)
    for line in lines:
        if line.startswith('key'):
            job = parse_table(lines, keytype=ttypes)
            jobs.append(job)
            byid[job.jobid] = job
        elif line.startswith('   jobid'):
            detail = parse_state_table(lines, snames)
            for jobid, fields in detail.items():
                setattr(byid[jobid], 'state', fields[1])

    if args.jobfile is not sys.stdin:
        args.jobfile.close()

    if args.output is not sys.stdout:
        with open(args.output, 'w') as out:
            pickle.dump((jobs, byid), out)
    else:
        pickle.dump((jobs, byid), args.output)
