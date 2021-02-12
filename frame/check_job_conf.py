import frame.util as util
import copy
import json
import os

log = util.Log()

class CheckJobConf():
    """用户配置检查类, 用于检查用户配置的合法性，并能注册用户自定义的检查"""
    def __init__(self):
        self._check_funcs_map = {}
        self._err_jobs = {}

    def register_user_check_func(self, func, argv):
        """注册用户自定义检查函数\n
        func: 用户实现的自定义配置检查函数\n
        argv: 自定义检查函数需要的参数，tuple类型"""
        self._check_funcs_map[func.__name__] = (func, argv)

    def _get_job_tasks(self, jobs):
        check_jobs = {}
        for job_id, job in jobs.items():
            check_jobs[job_id] = {}
            for task_key, task in job['tasks'].items():
                c_task = check_jobs[job_id]
                c_task[task_key] = {}
                c_task[task_key]['impl_name'] = task.get('impl_name', task_key)
                c_task[task_key]['upstreams'] = task.get('upstreams', [])
                if 'tasks' in task:
                    for ext_task_key, ext_task in task['tasks'].items():
                        c_task[ext_task_key] = {}
                        c_task[ext_task_key]['impl_name'] = ext_task.get('impl_name', ext_task_key)
                        ext_ups = copy.deepcopy(ext_task.get('upstreams', []))
                        ext_ups.extend(c_task[task_key]['upstreams'])
                        c_task[ext_task_key]['upstreams'] = ext_ups
        return check_jobs

    def _check_task_impl(self, jobs, task_cls_name_set):
        errs = {}
        valid_jobs = {}
        check_jobs = self._get_job_tasks(jobs)
        for job_id, job in check_jobs.items():
            err_tasks = []
            for task_key, info in job.items():
                if info['impl_name'] not in task_cls_name_set:
                    err_tasks.append([task_key])
            if len(err_tasks) > 0:
                errs[job_id] = err_tasks
            else:
                valid_jobs[job_id] = jobs[job_id]
        return (valid_jobs, errs)

    def _check_upstreams(self, jobs):
        errs = {}
        valid_jobs = {}
        check_jobs = self._get_job_tasks(jobs)
        max_loop = 50
        for job_id, job in check_jobs.items():
            err_tasks = []
            checked = set()
            loop = 0
            while True:
                loop = loop + 1
                if loop > max_loop:
                    break
                stop_flag = True
                for task_key, info in job.items():
                    ups = info['upstreams']
                    if len(ups) == 0: 
                        checked.add(task_key)
                        continue
                    all_in_checked = True
                    for up in ups:
                        if up not in checked:
                            all_in_checked = False
                    if all_in_checked:
                        checked.add(task_key)
                    else:
                        stop_flag = False
                if stop_flag:
                    break
            for task_key in job:
                if task_key not in checked:
                    err_tasks.append(task_key)
            if len(err_tasks) > 0:
                errs[job_id] = err_tasks
            else:
                valid_jobs[job_id] = jobs[job_id]
        return (valid_jobs, errs)

    def check_valid(self, jobs, task_cls_name_set):
        """执行配置检查，默认检查是否有task实现和task拓扑的正确性\n
        jobs: 所有任务配置
        task_cls_name_set: 已在task_impl_manager中注册的task实现类"""
        self.register_user_check_func(self._check_task_impl, (jobs, task_cls_name_set))
        self.register_user_check_func(self._check_upstreams, (jobs,))
        for name, func in self._check_funcs_map.items():
            log.log('INFO', 'do check:' + name)
            valid_jobs, errs = func[0](*func[1])
            if len(errs) > 0:
                self._err_jobs[name+'_errs'] = errs
            jobs = valid_jobs
        if len(self._err_jobs) > 0:
            log.log('ERROR', 'err_cfgs = {}'.format(self._err_jobs))
        return jobs, self._err_jobs