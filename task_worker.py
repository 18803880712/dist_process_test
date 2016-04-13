#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
***********************************
@ FileName : task_worker.py
@ Author   : qianfan
@ Data     : 2016/04/13
@ Email    : 18803880712@qq.com
@ Brief    : python 分布式进程函数的测试
***********************************
"""
import time,sys,Queue
from multiprocessing.managers import BaseManger

# 创建类似的QueueManager

class QueueManager(BaseManger):
    pass

# 由于QueueManger只从网络上获取Queue，所以注册时候只提供名字
QueueManager.register('get_task_queue')
QueueManager.register('get_result_queue')

# 连接到服务器,也就是运行taskmanager.py的机器

server_addr = '127.0.0.1'
print('Connect to server %s...' % server_addr)

# 端口号与验证码与task_manager.py设置保持完全一致

m = QueueManager(address=(server_addr,5000),authkey='abc')

# 从网络连接
m.connect()

# 获取Queue的对象
task = m.get_task_queue()
result = m.get_result_queue()

for i in range(10):
    try:
        n=task.get(timeout=1)
        print('run task %d * %d...' %(n,n))
        r = '%d * %d = %d' %(n,n,n*n)
        time.sleep(1)
        result.put(r)
    except Queue.Empty:
        print('task queue is empty')

# 处理结果

print('worker exit')
