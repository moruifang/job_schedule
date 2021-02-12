import os
from datetime import datetime
import frame.task_base
TaskBase = frame.task_base.TaskBase

demo_dir = os.path.dirname(__file__)

def get_full_name(sub_dir, fname):
    return demo_dir + "/" + sub_dir + "/" + fname

def common_group_over_do(groups, groups_res):
    it = iter(groups.keys())
    output = next(it).output
    with open(get_full_name('output', output), "w") as fp:
        for arg, res in groups_res.items():
            fp.write(''.join(res))

class Task1(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, ['flds'], {})

    def user_do(self, task_key, args, batch_jobs):
        flds = [int(x) for x in args.flds.split(',')]
        res = []
        with open(get_full_name('input', args.input)) as fp:
            for l in fp:
                fld_vals = l.split(',')
                sum = 0
                for fld in flds:
                    sum += int(fld_vals[fld])
                for job_id in batch_jobs:
                    res.append(','.join([job_id, fld_vals[0], fld_vals[1], str(sum/len(flds))])+'\n')
        return  res

    def task_groups_over(self, task_key, groups, groups_res, jobs):
        common_group_over_do(groups, groups_res)

class Task2(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, [], {})

    def user_do(self, task_key, args, batch_jobs):
        res = []
        with open(get_full_name('input', args.input)) as fp:
            for l in fp:
                fld_vals = l.split(',')
                for job_id in batch_jobs:
                    res.append(','.join([job_id, fld_vals[0], datetime.fromtimestamp(int(fld_vals[1])).strftime("%Y%m%d"), fld_vals[2]]))
        return  res

    def task_groups_over(self, task_key, groups, groups_res, jobs):
        common_group_over_do(groups, groups_res)


class Task3(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, ['flds', 'min', 'max'], {'min':0, 'max': 100})

    def user_do(self, task_key, args, batch_jobs):
        res = []
        with open(get_full_name('output', args.input)) as fp:
            for l in fp:
                fld_vals = l.split(',')
                job_id = fld_vals[0]
                val = int(fld_vals[3])
                if job_id in batch_jobs:
                    if val > args.min and val <= args.max:
                        res.append(l)
        return  res

    def task_groups_over(self, task_key, groups, groups_res, jobs):
        common_group_over_do(groups, groups_res)


class Task4(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, [], {})

    def user_do(self, task_key, args, batch_jobs):
        res = []
        with open(get_full_name('output', args.input)) as fp:
            for l in fp:
                fld_vals = l.split(',')
                job_id = fld_vals[0]
                if job_id in batch_jobs:
                    res.append(l)
        return  res

    def task_groups_over(self, task_key, groups, groups_res, jobs):
        common_group_over_do(groups, groups_res)


    def job_over_do(self, task_key, over_jobs, input):
        for job_id, job in over_jobs.items():
            with open(get_full_name('output', job['output']), "w") as w_fp:
                with open(get_full_name('output', input)) as r_fp:
                    for l in r_fp:
                        flds = l.split(",")
                        if flds[0] == job_id:
                            w_fp.write(','.join([flds[1], flds[2], flds[3]]))

    
class Task5(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, ['flds'], {})

    def user_do(self, task_key, args, batch_jobs):
        flds = [int(x) for x in args.flds.split(',')]
        res = []
        with open(get_full_name('input', args.input)) as fp:
            for l in fp:
                fld_vals = l.split(',')
                vals = [int(fld_vals[fld]) for fld in flds]
                for job_id in batch_jobs:
                    res.append(','.join([job_id, fld_vals[0], fld_vals[1], str(max(vals))])+'\n')
        return  res

    def task_groups_over(self, task_key, groups, groups_res, jobs):
        common_group_over_do(groups, groups_res)