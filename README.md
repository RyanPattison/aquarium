aquarium
========

Scripts for wrapping job management on SHARCNET.

# Makefiles

makefiles are used for several reasons:

- Output files can be deleted automatically if the run fails, or is killed for going overtime.
- They provide an easy interface to query if a job needs to be done using `-q`.
- Makefiles have key=value syntax for parameters making parameter files more readable.
- Makefiles allow for a job with many steps to be resubmitted and picked up where it left off.
- Makefiles will not run (and ruin the output of) programs that have already completed.

A Queue is used for each worker so that a single sharcnet job can consist of many tasks. A script is submitted as the job and this script runs through the queue file and runs tasks that have not yet completed. The script will not submit a task if it believes it will go over the limit. By having many tasks for a single job, the script will make better use of the alloted time. The hope is that a job will always produce some useful output, since even a bad estimate of the time it takes to complete 5 tasks should at least be enough to complete 1. In contrast to submitting separately, a bad estimate for 5 jobs will likely result in all of them failing.
