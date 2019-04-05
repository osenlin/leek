# -*- coding: utf-8 -*-
from auto_run import run_cluster
import multiprocessing
import datetime
import time
import sys

if __name__ == '__main__':
    '''
    start：python batch_task.py '2017-01-03' '2017-12-30' 1 5
    '''
    _, start_date_str, end_date_str, single_or_multi, num_semaphore = sys.argv
    start_date = datetime.datetime.strptime(start_date_str,"%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str,"%Y-%m-%d")
    num_semaphore = int(num_semaphore)
    semaphore = multiprocessing.Semaphore(num_semaphore)
    arr = []
    while (end_date-start_date).days >= 0 :
        arr.append(start_date.strftime("%Y-%m-%d"))
        start_date=start_date+datetime.timedelta(days=1)

    for item in arr:
        newtime = datetime.datetime.now()
        if single_or_multi == '1':
            print '---------run date [%s]----------' % item
            run_cluster('update', item)
        else:
            def runTask(semaphore, date):
                semaphore.acquire()
                datestr = date
                print '---------run date [%s]----------' % datestr
                run_cluster('update', datestr)
                semaphore.release()
            p = multiprocessing.Process(target=runTask, args=(semaphore, item))
            p.start()
        oldtime = datetime.datetime.now()
        print u'>>task success! spend {} seconds'.format((oldtime - newtime).seconds)