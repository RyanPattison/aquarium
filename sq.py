#!/usr/bin/env python

import sys
from subprocess import call, check_output
import argparse
import datetime
import pickle
import math


SUSPENDED = 'suspended'
KILLED = 'killed'
RUNNING = 'running'
QUEUED = 'queued'
DONE = 'done'
SLEEPING = 'sleeping'
WAITING = 'waiting'
UNKNOWN = 'unknown'

JOBS = (list(), dict())


#def call(s):
#  print "calling:", s


#def check_output(s):
#  print "checking output:", s
#  return "No output"


def sub(job):
  out = check_output('sqsub -r {} -o {} {}'.format(
    str_duration(job.cpu_time_limit),
    job.out_file, job.command))
  job.state = QUEUED
  for line in out.split():
    if line.startswith('submitted as jobid'):
      job.jobid = int(line.split()[-1])
      break
  return out


def kill(job):
  call('sqkill {}'.format(job.jobid))
  job.state = KILLED


def resume(job):
  call('sqresume {}'.format(job.jobid))


def jobs():
  return JOBS[0]


def suspend(job):
    call('sqsuspend {}'.format(job.jobid))
    job.state = SUSPENDED


def load(filename):
  global JOBS
  with open(filename, 'r') as p:
    JOBS = pickle.load(p)


def save(filename):
  global JOBS
  with open(filename, 'w') as out:
    pickle.dump(JOBS, out)


def update():
  global JOBS
  out = check_output('sqjobs -l')
  newjob, newid = parse_sqjobs(out.splitlines())
  for jobid, job in newid.items():
    if jobid in JOBS[1]:
      JOBS[1][jobid] = job
    else:
      JOBS[0].append(job)
      JOBS[1][jobid] = job
  return out


def job(command, time='1m', out_file='/dev/null'):
  j = argparse.Namespace()
  j.command = command
  j.out_file = out_file
  j.cpu_time_limit = duration(time)
  return j


def parse_args():
  parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
  parser.add_argument('jobfile', type=argparse.FileType('r'))
  parser.add_argument('output', type=argparse.FileType('w'))
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


def parse_state_table(header, lines):
  next(lines)
  details = dict()
  for line in lines:
    fields = line.split()[:3]
    st = state(fields[2])
    details[int(fields[0])] = st
  return details


def duration(s):
  durkey = dict(s=1,m=60,h=60*60,d=60*60*24)
  unit = s[-1]
  if unit in durkey:
      return datetime.timedelta(seconds=float(s[:-1]) * durkey[unit])
  else:
      return datetime.timedelta(minutes=float(s))


def str_duration(d):
  t = math.ceil(float(d.total_seconds())/60)
  unit = 'm'
  if t > 60:
    t /= 60.0
    unit = 'h'
    if t > 24:
      t /= 24.0
      unit = 'd'
  t = math.ceil(t * 100) / 100.0
  return '%.2f%s' %(t, unit)


def memsize(s):
  memkey = dict(K=1,M=2,G=3,T=4,P=5)
  return float(s[:-1]) * (1024 ** memkey[s[-1].upper()])


def date(s):
  dateformat = '%a %b %d %H:%M:%S %Y'
  return datetime.datetime.strptime(
            ' '.join(s.split()[:5]), dateformat)


def state(s):
  snames = dict(R=RUNNING, Q=QUEUED, Z=SLEEPING,
            S=SUSPENDED, K=KILLED, T=SUSPENDED,
            D=DONE)
  snames['*Q'] = WAITING
  return snames[s]


def parse_sqjobs(iterable):
  ttypes = {
        'jobid': int,
        'queue': str,
        'ncpus': int,
        'nodes': str,
        'command': str,
        'working directory': str,
        'out file': str,
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
  lines = iter(iterable)
  for line in lines:
    if line.startswith('key'):
      job = parse_table(lines, keytype=ttypes)
      jobs.append(job)
      byid[job.jobid] = job
    elif line.startswith('   jobid'):
      detail = parse_state_table(line, lines)
      for jobid, fields in detail.items():
        setattr(byid[jobid], 'state', fields)
  return jobs, byid


if __name__ == "__main__":
  args = parse_args()
  db = parse_sqjobs(args.jobfile)
  pickle.dump(db, args.output)
