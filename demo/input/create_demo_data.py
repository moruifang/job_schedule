import random
from datetime import datetime
import time
import os

input_dir = os.path.dirname(__file__)
file1 = input_dir + "/file1"
file2 = input_dir + "/file2"

with open(file1, 'w') as fp:
    for i in range(1000):
        uid = random.randint(1, 10)
        tm = int(time.time()) - random.randint(0, 86400*3)
        dt = datetime.fromtimestamp(tm).strftime("%Y%m%d")
        val1 = random.randint(1, 20)
        val2 = random.randint(1, 20)
        fp.write("{},{},{},{}\n".format(uid, dt, val1, val2))

with open(file2, 'w') as fp:
    for i in range(1000):
        uid = random.randint(1, 10)
        tm = int(time.time()) - random.randint(0, 86400*3)
        val3 = random.randint(1, 20)
        fp.write("{},{},{}\n".format(uid, tm, val3))


