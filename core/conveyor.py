"""Conveyor module downloading, archiving and saving file."""
import logging
import time
import zlib

from garcon import activity
from garcon import runner
from garcon import task
from garcon import utils
import requests

from core.config import BASEDIR

logger = logging.getLogger(__name__)

domain = 'MyTutorialDomain'
name = 'basic'
region = utils.get_region_info('us-west-2')
create = activity.create(domain, name, region=region)
# create = activity.create(domain, name)


class Carcass:
    """Class that represents temporary object for file."""

    body = None
    arch = None


carcass = Carcass()


def first_act(context, activity):
    """Describe activity downloading test files."""
    print('it has began')
    logger.info('begin to download the file')
    body = requests.get(url='https://example.com/').content
    logger.info('file has been downloaded')
    carcass.body = body
    return dict(first_act_result='file is downloaded')


@task.decorate(timeout=30)
def second_act(context, activity):
    """Describe activity compressing file."""
    print('it is happening')
    logger.info('ready to compress the file')
    time.sleep(120)
    carcass.arch = zlib.compress(carcass.body)
    logger.info('file is archived')
    return dict(second_act_result='file is compressed')


def third_act(context, activity):
    """Describe activity saving file to disk."""
    logger.info('ready to save archive to disk')
    with open(BASEDIR + '/humble_archive.gz', 'wb') as file:
        file.write(carcass.arch)
    logger.info('file is saved')
    print('it is done')
    return dict(third_act_result='file is saved')


test_activity_1 = create(
    name='activity_1',
    run=runner.Sync(first_act))

test_activity_2 = create(
    name='activity_2',
    requires=[test_activity_1],
    fail_on_timeout=True,
    run=runner.Sync(second_act))

test_activity_3 = create(
    name='activity_3',
    retry=10,
    requires=[test_activity_2],
    run=runner.Sync(third_act))
