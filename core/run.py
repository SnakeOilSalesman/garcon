"""Conveyor starting module."""
import logging
from threading import Thread
import time

import boto.swf.layer2 as swf
from garcon import activity
from garcon import decider

from core.config import BASEDIR
import core.conveyor as workflow

decider_worker = decider.DeciderWorker(workflow)


logging.basicConfig(
    filename=BASEDIR + '/core/main.log',
    filemode='w',
    level=logging.DEBUG)


swf.WorkflowType(
    name=workflow.name, domain=workflow.domain,
    version='1.0', task_list=workflow.name, region=workflow.region).start()


Thread(target=activity.ActivityWorker(workflow).run).start()
while(1):
    decider_worker.run()
    time.sleep(1)