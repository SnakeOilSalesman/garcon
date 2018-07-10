from __future__ import absolute_import
from __future__ import print_function
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import pytest

from garcon.boto_legacy import decisions_handler


@pytest.fixture
def vaild_continue_workflow_response():
    return {
        'decisionType': 'ContinueAsNewWorkflowExecution',
        'continueAsNewWorkflowExecutionDecisionAttributes': {
            'childPolicy': 'TERMINATE',
            'executionStartToCloseTimeout': '10',
            'input': 'input',
            'tagList': ['t1', 't2'],
            'taskList': {'name': 'tasklist'},
            'taskStartToCloseTimeout': '20',
            'workflowTypeVersion': 'v2',
        }
    }


def test_continue_as_new_workflow_execution(vaild_continue_workflow_response):
    data = vaild_continue_workflow_response
    d = decisions_handler.Decisions()

    d.continue_as_new_workflow_execution(
        child_policy='TERMINATE',
        execution_start_to_close_timeout='10',
        input='input',
        tag_list=['t1', 't2'],
        task_list='tasklist',
        start_to_close_timeout='20',
        workflow_type_version='v2'
    )
    assert d._data[0] == data
