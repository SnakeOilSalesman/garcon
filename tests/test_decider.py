from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import boto.swf.layer2 as swf
import json
import pytest

from garcon import decider
from garcon import activity
from tests.fixtures import decider as decider_events
from tests.fixtures import history as time_out_history


def mock(monkeypatch):
    for base in [swf.Decider, swf.WorkflowType, swf.ActivityType, swf.Domain]:
        monkeypatch.setattr(base, '__init__', MagicMock(return_value=None))
        if base is not swf.Decider:
            monkeypatch.setattr(base, 'register', MagicMock())


@pytest.fixture
def activities():
    class Activity:
        name = 'activity_name'
        task_list = 'task_list'
        region = None
    return [Activity(), Activity()]


def test_create_decider(monkeypatch):
    """Create a decider and check the behavior of the registration.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    assert len(d.activities) == 4
    assert d.flow
    assert d.domain
    assert d.on_exception
    assert d.region is None

    monkeypatch.setattr(decider.DeciderWorker, 'register', MagicMock())
    d = decider.DeciderWorker(example)
    assert d.register.called

    monkeypatch.setattr(decider.DeciderWorker, 'register', MagicMock())
    dec = decider.DeciderWorker(example, register=False)
    assert not dec.register.called

    monkeypatch.setattr(decider.DeciderWorker, 'register', MagicMock())
    monkeypatch.setattr(
        decider.DeciderWorker, 'get_registrable_entities', MagicMock())

    dec = decider.DeciderWorker(example)
    assert dec.register.called
    assert dec.get_registrable_entities.called


def test_create_decider_with_faulty_region_passed(monkeypatch):
    """Create a decider with region and check the behavior of the registration.
    """

    mock(monkeypatch)
    import tests.fixtures.flows.example_with_region as example
    monkeypatch.setattr(example, 'region', MagicMock(return_value=None))

    d = decider.DeciderWorker(example)
    assert len(d.activities) == 4
    assert d.flow
    assert d.domain
    assert d.on_exception
    assert d.region is None


def test_get_registrable_entities(monkeypatch, activities):
    """Test get entities with region that needed to be registered..
    """

    mock(monkeypatch)
    import tests.fixtures.flows.example_with_region as example

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)
    d = decider.DeciderWorker(example)
    d.version = 'version'
    d.activities = activities

    registrebales = d.get_registrable_entities()
    assert isinstance(registrebales, list)

    import boto.swf.layer2 as swf
    assert isinstance(registrebales[0], swf.Domain)
    assert isinstance(registrebales[1], swf.WorkflowType)

    for index in range(2, len(registrebales)):
        assert isinstance(registrebales[index], swf.ActivityType)


def test_check_activities_region(monkeypatch, activities):
    """Test get entities with region that needed to be registered..
    """

    mock(monkeypatch)
    import tests.fixtures.flows.example_with_region as example

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)

    for entity in activities:
        entity.region = example.region

    d = decider.DeciderWorker(example)
    d.flow = example
    d.version = 'version'
    d.region = example.region
    d.activities = activities

    result = d.activities_region_is_valid()
    assert result == True


def test_check_activities_region_fail(monkeypatch, activities):
    """Test activities region validation.
    """

    mock(monkeypatch)
    import tests.fixtures.flows.example_with_region as example

    activities[0].region = example.region

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)
    d = decider.DeciderWorker(example)
    d.flow = example
    d.version = 'version'
    d.region = example.region
    d.activities = activities

    result = d.activities_region_is_valid()
    assert result == False


def test_is_failed_activity_critical(monkeypatch):
    """Test failed activity validator behaviour.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)
    d = decider.DeciderWorker(example)
    d.critical = time_out_history.critical_activity_namelist

    event = time_out_history.time_out_event
    history = time_out_history.history

    result = d.is_failed_activity_critical(event, history)
    assert result == True


def test_is_failed_activity_critical_fail(monkeypatch):
    """Test failed activity validator behaviour in case of fail.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)
    d = decider.DeciderWorker(example)
    d.critical = []

    event = time_out_history.time_out_event
    history = time_out_history.history

    result = d.is_failed_activity_critical(event, history)
    assert result == False

    d.critical = time_out_history.not_timed_out_activity
    result = d.is_failed_activity_critical(event, history)
    assert result == False


def test_is_there_timed_out_activities(monkeypatch):
    """Test timed out activities seeker behaviour.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)
    monkeypatch.setattr(
        decider.DeciderWorker, 'is_failed_activity_critical',
        lambda self, *args, **kwargs: None)
    d = decider.DeciderWorker(example)

    history = time_out_history.history

    result = d.is_there_timed_out_activities(history)
    assert result is None


def test_is_there_timed_out_activities_fail(monkeypatch):
    """Test timed out activities seeker behaviour in case of fail.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(
        decider.DeciderWorker, '__init__', lambda self, *args, **kwargs: None)
    monkeypatch.setattr(
        decider.DeciderWorker, 'is_failed_activity_critical',
        lambda self, *args, **kwargs: True)

    d = decider.DeciderWorker(example)

    monkeypatch.setattr(swf, 'Layer1Decisions', MagicMock())
    monkeypatch.setattr(decider.DeciderWorker, 'complete', MagicMock())
    history = time_out_history.history

    with pytest.raises(Exception):
        d.is_there_timed_out_activities(history)

    assert d.complete.called
    assert swf.Layer1Decisions.called


def test_get_history(monkeypatch):
    """Test the decider history
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    events = decider_events.history.get('events')
    half = int(len(events) / 2)
    events = events[:half * 2]
    pool_1 = events[half:]
    pool_2 = events[:half]

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value={'events': pool_2})

    resp = d.get_history({'events': pool_1, 'nextPageToken': 'nextPage'})

    d.poll.assert_called_with(next_page_token='nextPage')
    assert len(resp) == len([
        evt for evt in events if evt['eventType'].startswith('Decision')])


def test_get_activity_states(monkeypatch):
    """Test get activity states from history.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    events = decider_events.history.get('events')
    d = decider.DeciderWorker(example)
    history = d.get_history({'events': events})
    states = d.get_activity_states(history)

    for activity_name, activity_instances in states.items():
        for activity_instance, activity_state in activity_instances.items():
            assert isinstance(activity_state, activity.ActivityState)


def test_running_workflow(monkeypatch):
    """Test running a workflow.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value=decider_events.history)
    d.complete = MagicMock()
    d.create_decisions_from_flow = MagicMock()

    # Via flow.
    d.run()
    assert d.create_decisions_from_flow.called

    # Via custom decider
    spy = MagicMock()

    def custom_decider(schedule):
        spy()

    example.decider = custom_decider
    d.run()
    assert spy.called


def test_running_workflow_identity(monkeypatch):
    """Test running a workflow with and without identity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    d.poll = MagicMock()
    d.complete = MagicMock()
    d.create_decisions_from_flow = MagicMock()

    # assert running decider without identity
    d.run()
    d.poll.assert_called_with(identity=None)

    # assert running decider with identity
    d.run('foo')
    d.poll.assert_called_with(identity='foo')


def test_running_workflow_exception(monkeypatch):
    """Run a decider with an exception raised during poll.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value=decider_events.history)
    d.complete = MagicMock()
    d.create_decisions_from_flow = MagicMock()
    exception = Exception('test')
    d.poll.side_effect = exception
    d.on_exception = MagicMock()
    d.logger.error = MagicMock()
    d.run()
    assert d.on_exception.called
    d.logger.error.assert_called_with(exception, exc_info=True)
    assert not d.complete.called


def test_create_decisions_from_flow_exception(monkeypatch):
    """Test exception is raised and workflow fails when exception raised.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    decider_worker = decider.DeciderWorker(example)
    decider_worker.logger.error = MagicMock()
    decider_worker.on_exception = MagicMock()

    exception = Exception('test')
    monkeypatch.setattr(decider.activity,
        'find_available_activities', MagicMock(side_effect = exception))

    mock_decisions = MagicMock()
    mock_activity_states = MagicMock()
    mock_context = MagicMock()
    decider_worker.create_decisions_from_flow(
        mock_decisions, mock_activity_states, mock_context)

    mock_decisions.fail_workflow_execution.assert_called_with(
        reason=str(exception))
    assert decider_worker.on_exception.called
    decider_worker.logger.error.assert_called_with(exception, exc_info=True)


def test_running_workflow_without_events(monkeypatch):
    """Test running a workflow without having any events.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value={})
    d.get_history = MagicMock()
    d.run()

    assert not d.get_history.called


def test_schedule_context():
    """Test the schedule context.
    """

    context = decider.ScheduleContext()
    assert context.completed

    context.mark_uncompleted()
    assert not context.completed


def test_schedule_with_unscheduled_activity(monkeypatch):
    """Test the scheduling of an activity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(decider, 'schedule_activity_task', MagicMock())

    decisions = MagicMock()
    schedule_context = decider.ScheduleContext()
    history = {}
    current_activity = example.activity_1

    decider.schedule(
        decisions, schedule_context, history, {}, 'schedule_id',
        current_activity)

    assert decider.schedule_activity_task.called
    assert not schedule_context.completed


def test_schedule_with_scheduled_activity(monkeypatch):
    """Test the scheduling of an activity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(decider, 'schedule_activity_task', MagicMock())

    decisions = MagicMock()
    schedule_context = decider.ScheduleContext()
    instance_state = activity.ActivityState('activity_1')
    instance_state.add_state(activity.ACTIVITY_SCHEDULED)
    current_activity = example.activity_1
    history = {
        current_activity.name: {
            'workflow_name_activity_1-1-schedule_id': instance_state
        }
    }

    resp = decider.schedule(
        decisions, schedule_context, history, {}, 'schedule_id',
        current_activity)

    assert not decider.schedule_activity_task.called
    assert not schedule_context.completed
    assert resp.get_last_state() == activity.ACTIVITY_SCHEDULED

    with pytest.raises(activity.ActivityInstanceNotReadyException):
        resp.result.get('foo')


def test_schedule_with_completed_activity(monkeypatch):
    """Test the scheduling of an activity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(decider, 'schedule_activity_task', MagicMock())

    decisions = MagicMock()
    schedule_context = decider.ScheduleContext()
    instance_state = activity.ActivityState('activity_1')
    instance_state.add_state(activity.ACTIVITY_COMPLETED)
    current_activity = example.activity_1
    history = {
        current_activity.name: {
            'workflow_name_activity_1-1-schedule_id': instance_state
        }
    }

    resp = decider.schedule(
        decisions, schedule_context, history, {}, 'schedule_id',
        current_activity)

    assert not decider.schedule_activity_task.called
    assert resp.get_last_state() == activity.ACTIVITY_COMPLETED
    assert schedule_context.completed
    resp.result.get('foo')


def test_schedule_requires_with_incomplete_activities():
    """Test the scheduler.
    """

    activity_state = activity.ActivityState('activity_name')
    with pytest.raises(activity.ActivityInstanceNotReadyException):
        decider.ensure_requirements([activity_state])

    with pytest.raises(activity.ActivityInstanceNotReadyException):
        decider.ensure_requirements([None])

    activity_state.add_state(activity.ACTIVITY_COMPLETED)
    decider.ensure_requirements(requires=[activity_state])


def test_schedule_activity_task(monkeypatch):
    """Test scheduling an activity task.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = MagicMock()
    decider.schedule_activity_task(decisions, instance)
    decisions.schedule_activity_task.assert_called_with(
        instance.id,
        instance.activity_name,
        '1.0',
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))


def test_schedule_activity_task_with_version(monkeypatch):
    """Test scheduling an activity task with a version.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = MagicMock()
    version = '2.0'
    decider.schedule_activity_task(decisions, instance, version=version)
    decisions.schedule_activity_task.assert_called_with(
        instance.id,
        instance.activity_name,
        version,
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))


def test_schedule_activity_task_with_version(monkeypatch):
    """Test scheduling an activity task with a custom id.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = MagicMock()
    custom_id = 'special_id'
    decider.schedule_activity_task(decisions, instance, id=custom_id)
    decisions.schedule_activity_task.assert_called_with(
        custom_id,
        instance.activity_name,
        '1.0',
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))
