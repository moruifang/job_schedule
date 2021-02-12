import os
import json
import threading

base_dir = os.path.dirname(__file__)
os.chdir(base_dir)

import frame.util as util
import frame.task_impl_manager as task_impl_manager
import frame.check_job_conf as check_job_conf
import frame.core as core
import demo.user_task as user_task

log = util.Log()
tim = task_impl_manager.TaskImplManager.get_instance()

#####user edit start#####
tim.register_task_class(user_task.Task1, ())
tim.register_task_class(user_task.Task2, ())
tim.register_task_class(user_task.Task3, ())
tim.register_task_class(user_task.Task4, ())
tim.register_task_class(user_task.Task5, ())

with open('demo/cfg/demo.json') as fp:
    jobs = json.load(fp)

output_path = 'demo/output/'
#####user edit over#####

cjc = check_job_conf.CheckJobConf()
valid_jobs, err_jobs = cjc.check_valid(jobs, tim.get_task_class_name_set())

core = core.Core()
level_jobs = core.merge_same_level_jobs(valid_jobs)
for level in level_jobs:
    threads = []
    for task_cls, task_info in level.items():
        log.log('INFO', 'do task ' + task_cls)
        t = threading.Thread(None, tim.get_imp_instance(task_cls).do_task, task_cls, (jobs, task_info))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

for tmp_file in tim.get_tmp_output_set():
    fname = output_path + tmp_file
    log.log('INFO', 'delete tmp file ' + fname)
    os.remove(fname)
