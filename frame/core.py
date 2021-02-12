import frame.util as util
import time
import copy
import os
import json
import frame.task_impl_manager as task_impl_manager
from collections import namedtuple

log = util.Log()
tim = task_impl_manager.TaskImplManager.get_instance()

class Core():
    """调度框架的核心类，主要实现对任务的拓扑分析以及分层合并"""
    def __init__(self):
        self._time_key = tim.get_timekey()

    def _parse_task_level(self, jobs):
        level_job_keys = {}
        global_max_level = 0
        for job_id, job in jobs.items():
            checked = set()
            level_job = [[]]
            while True:
                stop_flag = True
                for task_key, task in job['tasks'].items():
                    if task_key in checked:
                        continue
                    ups = task.get('upstreams', [])
                    if len(ups) == 0:
                        checked.add(task_key)
                        level_job[0].append(task_key)
                        task['level'] = 0
                        continue
                    max_up_level = 0
                    all_in_checked = True
                    for up in ups:
                        if up in checked:
                            up_task = job['tasks'][up]
                            max_up_level = up_task['level'] if up_task['level'] > max_up_level else max_up_level
                        else:
                            all_in_checked = False
                    if all_in_checked:
                        level = max_up_level + 1
                        if len(level_job) <= level:
                            level_job.append([])
                        checked.add(task_key)
                        level_job[level].append(task_key)
                        task['level'] = level
                    else:
                        stop_flag = False
                if stop_flag:
                        break
            job['max_level'] = len(level_job)
            global_max_level = global_max_level if global_max_level > job['max_level'] else job['max_level']
            level_job_keys[job_id] = level_job
        return (level_job_keys, global_max_level)

    def merge_same_level_jobs(self, jobs):
        """根据依赖关系，将相同level的任务放在一起\n
        jobs: 任务配置\n
        out: [{task实现类名:{task_key:{output:该task的临时输出文件名，job_ids:[该task_key包含的所有job_id]}}, ...}, {...}]"""
        level_job_idx = []
        level_job_keys, global_max_level = self._parse_task_level(jobs)
        log.log('DEBUG', 'level_job_keys = {}'.format(level_job_keys))
        for level_i in range(global_max_level):
            level_job_idx.append({})
            for job_id, job in jobs.items():
                if job['max_level'] <= level_i: 
                    continue
                for task_key in level_job_keys[job_id][level_i]:
                    output = 'tmp_{}_{}'.format(task_key, self._time_key)
                    if 'prefix' in job:
                        output = 'tmp_{}_{}_{}'.format(job['prefix'], task_key, self._time_key)
                    job['tasks'][task_key]['output'] =  output
                    impl_name = job['tasks'][task_key].get('impl_name', task_key)
                    level_job_idx[-1].setdefault(impl_name, {})
                    level_job_idx[-1][impl_name].setdefault(task_key, {'output': output})
                    level_job_idx[-1][impl_name][task_key].setdefault('job_ids', [])
                    level_job_idx[-1][impl_name][task_key]['job_ids'].append(job_id)
                    tim.add_tmp_output(output)
        return level_job_idx

    def group_by_same_args(self, jobs, task_key, task_key_info, arg_names, default_dict):
        """将同一个task的任务按照参数进行分组\n
        jobs: 任务配置\n
        task_key: job下的task的task_key\n
        task_key_info: {output:该task的临时输出文件名，job_ids:[该task_key包含的所有job_id]}}\n
        arg_names: 用户设置的用于分组的参数名\n
        default_dict: 参数的默认值\n
        out: {(参数值的namedtuple):[使用这组参数值的所有job_id], ...}，说明: 输入输出也会放入参数值tuple中，同一个task_key下各个分组的output一定是一样的"""
        groups = {}
        ext_args = ['input', 'output']
        Key = namedtuple('Key', ','.join(arg_names)+ ',' + ','.join(ext_args))
        for job_id in task_key_info['job_ids']:
            arg_vals = []
            job = jobs[job_id]
            for name in arg_names:
                arg_val = job[name] if name in job else ''
                if arg_val == '':
                    default_val = default_dict[name] if name in default_dict else ''
                    arg_val = job['tasks'][task_key].get(name, default_val)
                arg_vals.append(arg_val)
            inputs = set(job.get('inputs', '').split(','))
            task = job['tasks'][task_key]
            task_inputs = set(task.get('inputs', '').split(','))
            ignore_job_inputs = task.get('ignore_job_inputs', False)
            if ignore_job_inputs:
                inputs = task_inputs
            else:
                inputs.update(task_inputs)
            for up in task.get('upstreams', []):
                inputs.add(job['tasks'][up].get('output', ''))
            inputs.remove('')
            if len(inputs) == 0:
                arg_vals.append(task.get('input', ''))
                arg_vals.append(task.get('output', ''))
                key = Key(*arg_vals)
                groups.setdefault(key, [])
                groups[key].append(job_id)
            else:
                for input in inputs:
                    ext_arg_vals = copy.deepcopy(arg_vals)
                    ext_arg_vals.append(input)
                    ext_arg_vals.append(task.get('output', ''))
                    key = Key(*ext_arg_vals)
                    groups.setdefault(key, [])
                    groups[key].append(job_id)
        return groups
