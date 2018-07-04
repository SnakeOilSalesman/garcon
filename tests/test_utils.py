try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import datetime
import pytest

import botocore.exceptions as boto_exception

from garcon import utils


def test_create_dictionary_key_with_string():
    """Try creating a key using a string instead of a dict.
    """

    with pytest.raises(TypeError):
        utils.create_dictionary_key('something')


def test_create_dictionary_key_with_empty_dict():
    """Try creating a key using an empty dictionary.
    """

    with pytest.raises(ValueError):
        utils.create_dictionary_key(dict())


def test_create_dictionary_key():
    """Try creating a unique key from a dict.
    """

    values = [
        dict(foo=10),
        dict(foo2=datetime.datetime.now())]

    for value in values:
        assert len(utils.create_dictionary_key(value)) == 40

def test_create_dictionary_key():
    """Try creating a unique key from a dict.
    """

    values = [
        dict(foo=10),
        dict(foo2=datetime.datetime.now())]

    for value in values:
        assert len(utils.create_dictionary_key(value)) == 40


def test_non_throttle_error():
    """Assert SWF error is evaluated as non-throttle error properly.
    """

    response_status = 400
    response_reason = 'ThrottlingException'
    exception = boto_exception.ClientError(
        error_response={
            'Error': {
                'Code': response_status,
                'Message': response_reason
            },
        },
        operation_name='test_operation')

    assert not utils.non_throttle_error(exception)

    response_reason = 'OtheException'
    exception = boto_exception.ClientError(
        error_response={
            'Error': {
                'Code': response_status,
                'Message': response_reason
            },
        },
        operation_name='test_operation')
    assert utils.non_throttle_error(exception)

def test_throttle_backoff_handler():
    """Assert backoff is logged correctly.
    """

    mock_activity = MagicMock()
    details = dict(
        args=(mock_activity,),
        tries=5,
        wait=10)
    utils.throttle_backoff_handler(details)
    mock_activity.logger.info.assert_called_with(
        'Throttle Exception occurred on try {}. '
        'Sleeping for {} seconds'.format(
            details['tries'], details['wait']))
