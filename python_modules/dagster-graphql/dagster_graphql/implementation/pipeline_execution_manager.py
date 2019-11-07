from __future__ import absolute_import

import abc
import logging
import os
import signal
import sys
import time

import gevent
import six

from dagster import ExecutionTargetHandle, PipelineDefinition, PipelineExecutionResult, check
from dagster.core.errors import DagsterSubprocessError
from dagster.core.events import (
    DagsterEvent,
    DagsterEventType,
    PipelineProcessExitedData,
    PipelineProcessStartData,
    PipelineProcessStartedData,
)
from dagster.core.events.log import DagsterEventRecord
from dagster.core.execution.api import execute_run_iterator
from dagster.core.instance import DagsterInstance
from dagster.utils import get_multiprocessing_context, send_interrupt
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info


class PipelineExecutionManager(six.with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def execute_pipeline(self, handle, pipeline, pipeline_run, instance):
        '''Subclasses must implement this method.'''

    @abc.abstractmethod
    def terminate(self, run_id):
        '''Attempt to terminate a run if possible. Return False if unable to, True if it can.'''

    @abc.abstractmethod
    def can_terminate(self, run_id):
        '''Whether or not this execution manager can terminate the given run_id'''


def build_synthetic_pipeline_error_record(run_id, error_info, pipeline_name):
    check.str_param(run_id, 'run_id')
    check.str_param(pipeline_name, 'pipeline_name')
    check.inst_param(error_info, 'error_info', SerializableErrorInfo)

    return DagsterEventRecord(
        message=error_info.message + '\nStack Trace:\n' + '\n'.join(error_info.stack),
        # Currently it is the user_message that is displayed to the user client side
        # in dagit even though that was not the original intent. The original
        # intent was that the user_message was the message generated by user code
        # communicated directly to the client. We need to rationalize the treatment
        # of these different error messages
        user_message=(
            'An exception was thrown during execution that is likely a framework error, '
            'rather than an error in user code.'
        )
        + '\nOriginal error message: '
        + error_info.message
        + '\nStack Trace:\n'
        + '\n'.join(error_info.stack),
        level=logging.ERROR,
        run_id=run_id,
        timestamp=time.time(),
        error_info=error_info,
        pipeline_name=pipeline_name,
        dagster_event=DagsterEvent(DagsterEventType.PIPELINE_FAILURE.value, pipeline_name),
    )


def build_process_start_event(run_id, pipeline_name):
    check.str_param(pipeline_name, 'pipeline_name')
    check.str_param(run_id, 'run_id')
    message = 'About to start process for pipeline "{pipeline_name}" (run_id: {run_id}).'.format(
        pipeline_name=pipeline_name, run_id=run_id
    )

    return DagsterEventRecord(
        message=message,
        user_message=message,
        level=logging.INFO,
        run_id=run_id,
        timestamp=time.time(),
        error_info=None,
        pipeline_name=pipeline_name,
        dagster_event=DagsterEvent(
            message=message,
            event_type_value=DagsterEventType.PIPELINE_PROCESS_START.value,
            pipeline_name=pipeline_name,
            event_specific_data=PipelineProcessStartData(pipeline_name, run_id),
        ),
    )


def build_process_started_event(run_id, pipeline_name, process_id):
    message = 'Started process for pipeline (pid: {process_id}).'.format(process_id=process_id)

    return DagsterEventRecord(
        message=message,
        user_message=message,
        level=logging.INFO,
        run_id=run_id,
        timestamp=time.time(),
        error_info=None,
        pipeline_name=pipeline_name,
        dagster_event=DagsterEvent(
            message=message,
            event_type_value=DagsterEventType.PIPELINE_PROCESS_STARTED.value,
            pipeline_name=pipeline_name,
            step_key=None,
            solid_handle=None,
            step_kind_value=None,
            logging_tags=None,
            event_specific_data=PipelineProcessStartedData(
                pipeline_name=pipeline_name, run_id=run_id, process_id=process_id
            ),
        ),
    )


def build_process_exited_event(run_id, pipeline_name, process_id):
    message = 'Process for pipeline exited (pid: {process_id}).'.format(process_id=process_id)

    return DagsterEventRecord(
        message=message,
        user_message=message,
        level=logging.INFO,
        run_id=run_id,
        timestamp=time.time(),
        error_info=None,
        pipeline_name=pipeline_name,
        dagster_event=DagsterEvent(
            message=message,
            event_type_value=DagsterEventType.PIPELINE_PROCESS_EXITED.value,
            pipeline_name=pipeline_name,
            step_key=None,
            solid_handle=None,
            step_kind_value=None,
            logging_tags=None,
            event_specific_data=PipelineProcessExitedData(
                pipeline_name=pipeline_name, run_id=run_id, process_id=process_id
            ),
        ),
    )


class SynchronousExecutionManager(PipelineExecutionManager):
    def execute_pipeline(self, _, pipeline, pipeline_run, instance):
        check.inst_param(pipeline, 'pipeline', PipelineDefinition)

        event_list = []
        for event in execute_run_iterator(pipeline, pipeline_run, instance):
            event_list.append(event)
        return PipelineExecutionResult(pipeline, pipeline_run.run_id, event_list, lambda: None)

    def can_terminate(self, run_id):
        return False

    def terminate(self, run_id):
        return False


SUBPROCESS_TICK = 0.5


class SubprocessExecutionManager(PipelineExecutionManager):
    '''
    This execution manager launches a new process for every pipeline invocation.
    It tries to spawn new processes with clean state whenever possible,
    in order to pick up the latest changes, to not inherit in-memory
    state accumulated from the webserver, and to mimic standalone invocations
    of the CLI as much as possible.

    The exception here is unix variants before python 3.4. Before 3.4
    multiprocessing could not configure process start methods, so it
    falls back to system default. On unix variants that means it forks
    the process. This could lead to subtle behavior changes between
    python 2 and python 3.
    '''

    def __init__(self, instance):
        self._multiprocessing_context = get_multiprocessing_context()
        self._instance = instance
        self._living_process_by_run_id = {}
        self._processes_lock = self._multiprocessing_context.Lock()

        gevent.spawn(self._check_for_zombies)

    def _generate_synthetic_error_from_crash(self, run):
        try:
            raise Exception(
                'Pipeline execution process for {run_id} unexpectedly exited'.format(
                    run_id=run.run_id
                )
            )
        except Exception:  # pylint: disable=broad-except
            self._instance.handle_new_event(
                build_synthetic_pipeline_error_record(
                    run.run_id,
                    serializable_error_info_from_exc_info(sys.exc_info()),
                    run.pipeline_name,
                )
            )

    def _living_process_snapshot(self):
        with self._processes_lock:
            return {run_id: process for run_id, process in self._living_process_by_run_id.items()}

    def _check_for_zombies(self):
        '''
        This function polls the instance to synchronize it with the state of processes managed
        by this manager instance. On every tick (every 0.5 seconds currently) it gets the current
        index of run_id => process and sees if any of them are dead. If they are, then it queries
        the instance to see if the runs are in a proper terminal state (success or failure). If
        not, then we can assume that the underlying process died unexpected and clean everything.
        In either case, the dead process is removed from the run_id => process index.
        '''
        while True:
            runs_to_clear = []

            living_process_snapshot = self._living_process_snapshot()

            for run_id, process in living_process_snapshot.items():
                if not process.is_alive():
                    run = self._instance.get_run_by_id(run_id)
                    if not run:  # defensive
                        continue

                    runs_to_clear.append(run_id)

                    # expected terminal state. it's fine for process to be dead
                    if run.is_finished:
                        continue

                    # the process died in an unexpected manner. inform the system
                    self._generate_synthetic_error_from_crash(run)

            with self._processes_lock:
                for run_to_clear_id in runs_to_clear:
                    del self._living_process_by_run_id[run_to_clear_id]

            gevent.sleep(SUBPROCESS_TICK)

    def execute_pipeline(self, handle, pipeline, pipeline_run, instance):
        '''Subclasses must implement this method.'''
        check.inst_param(handle, 'handle', ExecutionTargetHandle)

        mp_process = self._multiprocessing_context.Process(
            target=_in_mp_process,
            kwargs={
                'handle': handle,
                'pipeline_run': pipeline_run,
                'instance_ref': instance.get_ref(),
            },
        )

        instance.handle_new_event(build_process_start_event(pipeline_run.run_id, pipeline.name))
        mp_process.start()

        with self._processes_lock:
            self._living_process_by_run_id[pipeline_run.run_id] = mp_process

    def join(self):
        with self._processes_lock:
            for run_id, process in self._living_process_by_run_id.items():
                if process.is_alive():
                    process.join()

                run = self._instance.get_run_by_id(run_id)

                if run and not run.is_finished:
                    self._generate_synthetic_error_from_crash(run)

    def _get_process(self, run_id):
        with self._processes_lock:
            return self._living_process_by_run_id.get(run_id)

    def is_process_running(self, run_id):
        check.str_param(run_id, 'run_id')
        process = self._get_process(run_id)
        return process.is_alive() if process else False

    def can_terminate(self, run_id):
        check.str_param(run_id, 'run_id')

        process = self._get_process(run_id)

        if not process:
            return False

        if not process.is_alive():
            return False

        return True

    def terminate(self, run_id):
        check.str_param(run_id, 'run_id')

        process = self._get_process(run_id)

        if not process:
            return False

        if not process.is_alive():
            return False

        send_interrupt(process.pid)
        process.join()
        return True


def _in_mp_process(handle, pipeline_run, instance_ref):
    """
    Execute pipeline using message queue as a transport
    """
    run_id = pipeline_run.run_id
    pipeline_name = pipeline_run.pipeline_name

    instance = DagsterInstance.from_ref(instance_ref)
    instance.handle_new_event(build_process_started_event(run_id, pipeline_name, os.getpid()))

    try:
        handle.build_repository_definition()
        pipeline_def = handle.with_pipeline_name(pipeline_name).build_pipeline_definition()
    except Exception:  # pylint: disable=broad-except
        repo_error = sys.exc_info()
        instance.handle_new_event(
            build_synthetic_pipeline_error_record(
                run_id, serializable_error_info_from_exc_info(repo_error), pipeline_name
            )
        )
        return

    try:
        event_list = []
        for event in execute_run_iterator(
            pipeline_def.build_sub_pipeline(pipeline_run.selector.solid_subset),
            pipeline_run,
            instance,
        ):
            event_list.append(event)
        return PipelineExecutionResult(pipeline_def, run_id, event_list, lambda: None)

    # Add a DagsterEvent for unexpected exceptions
    # Explicitly ignore KeyboardInterrupts since they are used for termination
    except DagsterSubprocessError as err:
        if not all(
            [err_info.cls_name == 'KeyboardInterrupt' for err_info in err.subprocess_error_infos]
        ):
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            instance.handle_new_event(
                build_synthetic_pipeline_error_record(run_id, error_info, pipeline_name)
            )
    except Exception:  # pylint: disable=broad-except
        error_info = serializable_error_info_from_exc_info(sys.exc_info())
        instance.handle_new_event(
            build_synthetic_pipeline_error_record(run_id, error_info, pipeline_name)
        )
    finally:
        instance.handle_new_event(build_process_exited_event(run_id, pipeline_name, os.getpid()))
