from __future__ import absolute_import
from __future__ import print_function
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import json
import pytest

import boto3
from botocore.exceptions import ClientError

from garcon.boto_legacy import medium


@pytest.fixture
def common_base_valid_parameters():
    return {
        'name': 'name',
        'domain': 'domain',
        'aws_access_key_id': 'id',
        'aws_secret_access_key': 'key',
        'region': 'region',
    }

@pytest.fixture
def poll_for_activity_task_response():
    return {
        'activityId': 'SomeActivity-1379020713',
        'activityType': {'name': 'SomeActivity', 'version': '1.0'},
        'startedEventId': 6,
        'taskToken': '',
        'workflowExecution': {
            'runId': '12T026NzGK5c4eMti06N9O3GHFuTDaNyA+8LFtoDkAwfE=',
            'workflowId': 'MyWorkflow-1.0-1379020705'}}

@pytest.fixture
def poll_for_decision_task_response():
    return {
        'events': [
            {
                'eventId': 1,
                'eventTimestamp': 1379019427.953,
                'eventType': 'WorkflowExecutionStarted',
                'workflowExecutionStartedEventAttributes': {
                'childPolicy': 'TERMINATE',
                'executionStartToCloseTimeout': '3600',
                'parentInitiatedEventId': 0,
                'taskList': {'name': 'test_list'},
                'taskStartToCloseTimeout': '123',
                'workflowType': {'name': 'test_workflow_name',
                'version': 'v1'}}
            },
            {'decisionTaskScheduledEventAttributes':
                 {
                     'startToCloseTimeout': '123',
                     'taskList': {'name': 'test_list'}
                 },
                 'eventId': 2,
                 'eventTimestamp': 1379019427.953,
                 'eventType': 'DecisionTaskScheduled'
            },
            {
                'decisionTaskStartedEventAttributes': {'scheduledEventId': 2},
                 'eventId': 3, 'eventTimestamp': 1379019495.585,
                 'eventType': 'DecisionTaskStarted'
            }
        ],
         'previousStartedEventId': 0, 'startedEventId': 3,
         'taskToken': '',
         'workflowExecution': {'runId': 'fwr243dsa324132jmflkfu0943tr09=',
                   'workflowId': 'test_workflow_name-v1-1379019427'},
         'workflowType': {'name': 'test_workflow_name', 'version': 'v1'}
    }

@pytest.fixture
def activity_object():
    class ActvitityDummy:
        name = 'test_activity_name'

    return ActvitityDummy()


def test_common_base(common_base_valid_parameters):
    """Test common data base class.
    """

    boto3.client = MagicMock()

    base = medium.CommonBase()
    assert base.name == None
    assert base.domain == None
    assert base.aws_access_key_id == None
    assert base.aws_secret_access_key == None
    assert base.region == None

    base = medium.CommonBase(
        name='name',
        domain='domain',
        aws_access_key_id='id',
        aws_secret_access_key='key',
        region='region')
    assert base.name == common_base_valid_parameters['name']
    assert base.domain == common_base_valid_parameters['domain']
    assert base.aws_access_key_id == common_base_valid_parameters[
        'aws_access_key_id']
    assert base.aws_secret_access_key == common_base_valid_parameters[
        'aws_secret_access_key']
    assert base.region == common_base_valid_parameters['region']


def test_activity_class(poll_for_activity_task_response):
    """Test Activity Worker class.
    """

    boto3.client = MagicMock()

    worker = medium.ActivityWorker()
    assert worker.name == None
    assert worker.domain == None
    assert worker.aws_access_key_id == None
    assert worker.aws_secret_access_key == None
    assert worker.region == None
    pass

    worker = medium.ActivityWorker(
        name='test_name',
        domain='test_domain',
        task_list='test_task_list')
    assert worker.name == 'test_name'
    assert worker.domain == 'test_domain'
    assert worker.task_list == 'test_task_list'

    task_token = 'worker_task_token'
    input_data = poll_for_activity_task_response
    input_data['taskToken'] = task_token
    worker._swf.poll_for_activity_task.return_value = input_data

    worker.poll()

    worker.cancel(details='Cancelling!')
    worker.complete(result='Done!')
    worker.fail(reason='Failure!')
    worker.heartbeat()

    worker._swf.respond_activity_task_canceled.assert_called_with(
        taskToken=task_token, details='Cancelling!')
    worker._swf.respond_activity_task_completed.assert_called_with(
        taskToken=task_token, result=json.dumps('Done!'))
    worker._swf.respond_activity_task_failed.assert_called_with(
        taskToken=task_token, details='', reason='Failure!')
    worker._swf.record_activity_task_heartbeat.assert_called_with(
        taskToken=task_token, details='')


def test_decider_class(poll_for_decision_task_response):
    """Test Decider Worker class.
    """

    boto3.client = MagicMock()

    decider = medium.Decider()

    task_token = 'my_specific_task_token'
    input_data = poll_for_decision_task_response
    input_data['taskToken'] = task_token
    decider._swf.poll_for_decision_task.return_value = input_data

    decider.poll()
    decider.complete()

    decider._swf.respond_decision_task_completed.assert_called_with(
        taskToken=task_token, decisions=None)
    assert decider.last_tasktoken == 'my_specific_task_token'


def test_actor_poll_without_tasklist_override():
    """Test Actors behaviour without task list override.
    """

    boto3.client = MagicMock()

    worker = medium.ActivityWorker(
        name='test_name',
        domain='test_domain',
        task_list='test_task_list')
    decider = medium.Decider(
        name='test_name',
        domain='test_domain',
        task_list='test_task_list')
    worker.poll()
    decider.poll()
    worker._swf.poll_for_activity_task.assert_called_with(
        domain='test_domain', taskList={'name': 'test_task_list'})
    decider._swf.poll_for_decision_task.assert_called_with(
        domain='test_domain', taskList={'name': 'test_task_list'}, identity='')


def test_worker_override_tasklist():
    """Test ActivityWorker behaviour with task list override.
    """

    boto3.client = MagicMock()

    worker = medium.ActivityWorker(
        name='test_name',
        domain='test_domain',
        task_list='test_task_list')
    worker.poll(task_list='some_other_tasklist')
    worker._swf.poll_for_activity_task.assert_called_with(
        domain='test_domain', taskList={'name': 'some_other_tasklist'})


def test_decider_override_tasklist():
    """Test Decider behaviour without task list override.
    """

    boto3.client = MagicMock()

    decider = medium.Decider(
        name='test_name',
        domain='test_domain',
        task_list='test_task_list')
    decider.poll(task_list='some_other_tasklist')
    decider._swf.poll_for_decision_task.assert_called_with(
        domain='test_domain', taskList={'name': 'some_other_tasklist'},
        identity='')


def test_register_instance_with_given_method():
    """Test Decider's instances registerer method.
    """

    boto3.client = MagicMock()

    decider = medium.Decider()
    method = MagicMock()
    data = [{'some_key': 'some_data'}]
    decider.register_instance_with_given_method(method, data)
    method.assert_called_once_with(**data[0])


def test_register_instance_with_given_method_exception():
    """Test Decider's instances registerer method raising error.
    """

    boto3.client = MagicMock()

    decider = medium.Decider()

    method = MagicMock()
    mocked_print = medium.print = MagicMock()
    exception = ClientError(
        error_response={
            'Error': {
                'Code': 'test_error_code',
                'Message': 'test_error_message'
            },
        },
        operation_name='test_operation')
    method.side_effect = exception
    data = [{'some_key': 'some_data'}]

    decider.register_instance_with_given_method(method, data)
    method.assert_called_with(**data[0])
    mocked_print.assert_called_once_with(exception)


def test_register(activity_object):
    """Test Decider's instances registerer.
    """

    boto3.client = MagicMock()
    medium.Decider.register_instance_with_given_method = MagicMock()

    decider = medium.Decider(
        name='test_name',
        domain='test_domain',
        version='test_version')
    decider.activities = [activity_object, activity_object]

    decider.register()
