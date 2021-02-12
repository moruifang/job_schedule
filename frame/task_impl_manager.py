import frame.util as util
import time
import threading

log = util.Log()

class TaskImplManager():
    """管理task实现的类，该类为单例模式，使用get_instance获取实例"""
    _lock = threading.Lock()

    def __init__(self):
        self._task_cls_map = {}
        self._time_key = int(time.time())
        self._tmp_output_set = set()

    @classmethod
    def get_instance(cls):
        """获取实例\n
        out: TaskImplManager实例"""
        if not hasattr(cls, '_instance'):
            with TaskImplManager._lock:
                if not hasattr(cls, '_instance'):
                    cls._instance = TaskImplManager()
        return cls._instance

    def register_task_class(self, task_cls, argv):
        """注册task的实现类\n
        task_cls: 实现task的类名\n
        argv: task类的实例化参数，tuple类型\n"""
        key = task_cls.__name__
        self._task_cls_map[key] = (task_cls, argv)

    def get_imp_instance(self, key):
        """获取task实现实例\n
        key: 实现类的类名
        out: 实现类实例"""
        cls_tuple = self._task_cls_map.get(key, None)
        if cls_tuple == None:
            return None
        return cls_tuple[0](*cls_tuple[1])

    def get_task_class_name_set(self):
        """获取已注册的所有实现类的类名\n
        out: 已注册的所有实现类的类名, set类型"""
        return set(self._task_cls_map.keys())
    
    def reset_timekey(self, time_key):
        """time_key作为每次jobs执行的key，临时文件用次key作为防止重名，一般为时间戳，但可以用户自定义\n
        time_key: 不设置使用时间戳，设置使用该设置值"""
        if time_key == None:
            self._time_key = int(time.time())
        else:
            self._time_key = time_key

    def get_timekey(self):
        """time_key作为每次jobs执行的key，临时文件用次key作为防止重名，一般为实例初始化的时间戳\n
        获取timekey\n
        out: timekey"""
        return self._time_key

    def get_tmp_output_set(self):
        """获取任务执行过程中所有使用的临时文件名
        out: 临时文件名 set类型"""
        return self._tmp_output_set

    def add_tmp_output(self, output):
        """添加临时文件名，用于管理用户使用的临时文件，便于整体删除
        output: 用户使用的临时文件名"""
        with TaskImplManager._lock:
            self._tmp_output_set.add(output)