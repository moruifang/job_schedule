import copy
import frame.util as util
import frame.core as core
import frame.task_impl_manager as task_impl_manager

log = util.Log()
tim = task_impl_manager.TaskImplManager.get_instance()

class TaskBase():
    """task实现的基类，所有用户的task实现都要继承该类\n
    初始化参数\n
    arg_names: 用于对同一个task_key进行任务分组的参数名\n
    default_dict: 参数值的默认值"""
    def __init__(self, arg_names, default_dict):
        self._core = core.Core()
        self._arg_names = arg_names
        self._default_dict = default_dict

    def task_groups_startup(self, task_key, groups, jobs):
        """所有分组开始执行前执行的操作\n
        task_key: 该task_key下按照参数值对任务做的分组\n
        groups: 任务分组,{(参数值的namedtuple):[使用这组参数值的所有job_id], ...}\n
        jobs: 任务所有配置"""
        log.log('DEBUG', 'do groups startup: groups = {}'.format(groups))

    def user_do(self, task_key, args, batch_jobs):
        """对同一组参数的分组执行的操作\n
        task_key: task的task_key\n
        args: 参数namedtuple\n
        batch_jobs: 该task_key与参数涉及的所有task的配置数据\n
        out: 执行产出的数据，用户定义"""
        log.log('DEBUG', 'do user func: args = {}'.format(args))

    def task_groups_over(self, task_key, groups, groups_res, jobs):
        """所有分组执行结束后的操作\n
        task_key: task的task_key\n
        groups: 任务分组,{(参数值的namedtuple):[使用这组参数值的所有job_id], ...}\n
        groups_res: user_do执行产出的数据,{(参数值的namedtuple):user_do的返回结果, ...}\n
        jobs: 所有任务配置"""
        log.log('DEBUG', 'do groups over: groups_res = {}'.format(groups_res))

    def job_over_do(self, task_key, over_jobs, input):
        """整个任务结束后做的操作，如果该task是整个拓扑中最后一个level的task则执行\n
        task_key: task的task_key\n
        over_jobs: 该task_key是最后一个level task_key涉及到的任务配置\n
        input: 执行该操作的输入数据"""
        log.log('DEBUG', 'do job over: task_key = {}, input = {}')

    def _get_sub_jobs(self, task_key, batch_jobs):
        sub_jobs = {job_id : job for job_id, job in batch_jobs.items() if 'tasks' in job['tasks'][task_key]}
        return sub_jobs

    def user_set_sub_jobs_conf(self, task_key, sub_jobs):
        """如果task中包含子任务，则在该函数配置，哪些子任务是需要执行的，如果不需要执行，则在相应的task中配置ignore = True\n
        task_key: task的task_key\n
        sub_jobs: task中包括sub task的任务配置"""
        log.log('DEBUG', 'user edit conf which depends on its parent result')

    def _do_sub_tasks(self, task_key, batch_jobs):
        sub_jobs = {}
        done_jobs = {}
        for job_id, job in batch_jobs.items():
            task = job['tasks'][task_key]
            if 'ignore' in task:
                continue
            new_job = copy.deepcopy(job)
            new_job.update(copy.deepcopy(task))
            new_job['sub_job_key'] = task_key+'_sub'
            for sub_task_key, sub_task in task['tasks'].items():
                for up in sub_task.get('upstreams', []):
                    if up in job['tasks']:
                        done_jobs.setdefault(up, [])
                        done_jobs[up].append(job_id)
                        new_job['tasks'][up] = copy.deepcopy(job['tasks'][up])
            sub_jobs[job_id] = new_job
        if len(sub_jobs) == 0:
            return
        log.log('DEBUG', 'sub_jobs = {}'.format(sub_jobs))
        log.log('DEBUG', 'done_jobs = {}'.format(done_jobs))
        level_jobs = self._core.merge_same_level_jobs(sub_jobs)
        for level in level_jobs:
            for task_cls, task_info in level.items():
                log.log('INFO', 'do sub_task ' + task_cls)
                for task_key, job_ids in done_jobs.items():
                    if task_key in task_info:
                        new_job_ids = [job_id for job_id in task_info[task_key]['job_ids'] if job_id not in job_ids]
                        task_info[task_key]['job_ids'] = new_job_ids
                log.log('DEBUG', 'task_info = {}'.format(task_info))
                tim.get_imp_instance(task_cls).do_task(sub_jobs, task_info)

    def do_task(self, jobs, task_keys):
        """执行task\n
        jobs: 所有任务配置\n
        task_keys: task的task_key"""
        for task_key, task_key_info in task_keys.items():
            if len(task_key_info['job_ids']) == 0:
                log.log('INFO', '{} job_ids is empty, do nothing'.format(task_key))
                continue
            groups = self._core.group_by_same_args(jobs, task_key, task_key_info, self._arg_names, self._default_dict)
            self.task_groups_startup(task_key, groups, jobs)
            over_jobs = {}
            check_sub_jobs = {}
            groups_res = {}
            for args, job_ids in groups.items():
                batch_jobs = {}
                for job_id in job_ids:
                    batch_jobs[job_id] = jobs[job_id]
                    if jobs[job_id]['tasks'][task_key]['level'] == jobs[job_id]['max_level'] - 1:
                        over_jobs[job_id] = jobs[job_id]
                    check_sub_jobs[job_id] = jobs[job_id]
                res = self.user_do(task_key, args, batch_jobs)
                groups_res[args] = res
            self.task_groups_over(task_key, groups, groups_res, jobs)
            sub_jobs = self._get_sub_jobs(task_key, check_sub_jobs)
            if len(sub_jobs) > 0:
                self.user_set_sub_jobs_conf(task_key, sub_jobs)
                self._do_sub_tasks(task_key, sub_jobs)
            if len(over_jobs) >0:
                self.job_over_do(task_key, over_jobs, task_key_info['output'])





