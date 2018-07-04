# -*- coding: utf-8 -*-
"""
Medium
========

Middle-level SWF interface. Adopted from Layer2 boto2 SWF interface.

"""

import json

from botocore.exceptions import ClientError
import boto3

from garcon import decisions_handler

DEFAULT_REGION = 'us-east-1'
DEFAULT_RETENTION_PERIOD = '30'
DEFAULT_MAXIMUM_PAGE_SIZE = 1000


class CommonBase:
    """Class representing common data base of the middle-level SWF interface.
    """

    name = None
    domain = None
    aws_access_key_id = None
    aws_secret_access_key = None
    region = None

    def __init__(self, **kwargs):
        """Provide class with initial config data.
        """

        for kwarg in kwargs:
            setattr(self, kwarg, kwargs[kwarg])

        self._swf = boto3.client(
            service_name='swf',
            region_name=self.region or DEFAULT_REGION,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )


class Actor(CommonBase):

    task_list = None
    last_tasktoken = None

    def run(self):
        """To be overloaded by subclasses."""
        raise NotImplementedError()


class ActivityWorker(Actor):

    def poll(self, task_list=None, identity=None):
        """PollForActivityTask."""
        additional_kwargs = dict(identity=identity) if identity else dict()
        if task_list:
            self.task_list = task_list
        task = self._swf.poll_for_activity_task(
            domain=self.domain,
            taskList=dict(name=self.task_list),
            **additional_kwargs)

        self.last_tasktoken = task.get('taskToken')
        return task

    def heartbeat(self, task_token=None, details=''):
        """RecordActivityTaskHeartbeat."""
        return self._swf.record_activity_task_heartbeat(
            taskToken=task_token or self.last_tasktoken,
            details=details)

    def complete(self, task_token=None, result=None):
        """RespondActivityTaskCompleted."""
        self._swf.respond_activity_task_completed(
            taskToken=task_token or self.last_tasktoken,
            result=json.dumps(result))

    def fail(self, task_token=None, details=None, reason=None):
        """RespondActivityTaskFailed."""
        self._swf.respond_activity_task_failed(
            taskToken=task_token or self.last_tasktoken,
            reason=reason if reason else '',
            details=details or '')

    def cancel(self, task_token=None, details=None):
        """RespondActivityTaskCanceled."""
        if task_token is None:
            task_token = self.last_tasktoken
        self._swf.respond_activity_task_canceled(
            taskToken=task_token or self.last_tasktoken,
            details=details or '')


class Decider(Actor):

    version = None
    activities = None

    def poll(self, identity=None, task_list=None, next_page_token=None,
            maximum_page_size=None, reverse_order=None):
        """PollForDecisionTask."""

        additional_kwargs = dict()
        if next_page_token:
            additional_kwargs['nextPageToken'] = next_page_token
        if maximum_page_size:
            additional_kwargs['maximumPageSize'] = maximum_page_size
        if reverse_order:
            additional_kwargs['reverseOrder'] = reverse_order

        if task_list:
            self.task_list = task_list

        decision_task = self._swf.poll_for_decision_task(
            domain=self.domain,
            taskList=dict(name=self.task_list),
            identity=identity or '',
            **additional_kwargs)

        self.last_tasktoken = decision_task.get('taskToken')
        return decision_task

    def complete(self, task_token=None, decisions=None, **kwargs):
        """RespondDecisionTaskCompleted."""
        if isinstance(decisions, decisions_handler.Decisions):
            decisions = decisions._data

        self._swf.respond_decision_task_completed(
            taskToken=task_token or self.last_tasktoken,
            decisions=decisions)

    def register(self):
        """Register the Workflow on SWF.

        To work, SWF needs to have pre-registered the domain, the workflow,
        and the different activities, this method takes care of this part.
        """

        registerable = []
        registerable.append(dict(
            method=self._swf.register_domain,
            data=[{
                'name': self.domain,
                'workflowExecutionRetentionPeriodInDays':
                    DEFAULT_RETENTION_PERIOD}]))

        registerable.append(dict(
            method=self._swf.register_workflow_type,
            data=[{
                'domain': self.domain,
                'name': self.task_list,
                'version': self.version}]))

        registerable.append(dict(
            method=self._swf.register_activity_type,
            data=[{
                'domain': self.domain,
                'name': single_activity.name,
                'version': self.version
            } for single_activity in self.activities]))

        for instance in registerable:
            self.register_instance_with_given_method(**instance)

    def register_instance_with_given_method(self, method, data):
        """Register AWS SWF element with given specific method.

        Every SWF element has it's own specific method of registration.
        This method is independent from the type of element and uses given
        method to register every element from given data.

        Args:
            method (callable): specific method needed to register the element.
            data (list): list of data of elements needed to be registered.
        """

        for entity in data:
            try:
                method(**entity)
            except ClientError as e:
                print(e)
