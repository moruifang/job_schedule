#!encoding=utf-8
import os
import inspect
import threading
import sys
from datetime import datetime

class Log():
    """日志类，修改print_level控制日志打印级别"""
    _print_level = 3 
    _log_level = {'ERROR': 0, 'WARNING': 1, 'INFO': 2, 'TRACE':3, 'DEBUG': 4}

    def _get_stack(self):
        stacks = inspect.stack()[2:6]
        s_list = []
        for s in stacks:
            fname = os.path.basename(s[1])
            s_list.append(fname+':'+str(s[2]))
        return ','.join(s_list)

    def log(self, level, msg):
        """写日志，日志输出级别依赖_print_level\n
        level: ERROR，WARNING, INFO, TRACE, DEBUG 中的一种\n
        msg: 输出的日志信息"""
        level = level.upper()
        if level not in self._log_level or self._log_level[level] > self._print_level:
            return
        tm = datetime.now().strftime("%Y%m%d %H:%M:%S")
        t = threading.currentThread()
        stack = self._get_stack()
        print("[{:<5}] [{}] [{}] [{}] {}".format(level, tm, t.ident, stack, msg))
        sys.stdout.flush()
        