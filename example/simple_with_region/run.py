from garcon import activity
from garcon import decider
from threading import Thread
import boto.swf.layer2 as swf
import time

from example.simple_with_region import workflow

deciderworker = decider.DeciderWorker(workflow)

swf.WorkflowType(
    name=workflow.name, domain=workflow.domain,
    version='1.0', task_list=workflow.name, region=workflow.region).start()

Thread(target=activity.ActivityWorker(workflow).run).start()
while(True):
    deciderworker.run()
    time.sleep(1)
