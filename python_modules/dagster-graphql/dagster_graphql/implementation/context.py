from dagster import ExecutionTargetHandle, check
from dagster.core.instance import DagsterInstance

from .pipeline_execution_manager import PipelineExecutionManager
from .reloader import Reloader


class DagsterGraphQLContext(object):
    def __init__(self, handle, execution_manager, instance, reloader=None, version=None):
        self._handle = check.inst_param(handle, 'handle', ExecutionTargetHandle)
        self.instance = check.inst_param(instance, 'instance', DagsterInstance)
        self.reloader = check.opt_inst_param(reloader, 'reloader', Reloader)
        self.execution_manager = check.inst_param(
            execution_manager, 'pipeline_execution_manager', PipelineExecutionManager
        )
        self.version = version
        self.repository_definition = self.get_handle().build_repository_definition()

        self.scheduler_handle = self.get_handle().build_scheduler_handle(
            artifacts_dir=self.instance.schedules_directory()
        )

    def get_scheduler(self):
        return self.scheduler_handle.get_scheduler() if self.scheduler_handle else None

    def get_handle(self):
        return self._handle

    def get_repository(self):
        return self.repository_definition

    def get_pipeline(self, pipeline_name):
        orig_handle = self.get_handle()
        if orig_handle.is_resolved_to_pipeline:
            pipeline_def = orig_handle.build_pipeline_definition()
            check.invariant(
                pipeline_def.name == pipeline_name,
                '''Dagster GraphQL Context resolved pipeline with name {handle_pipeline_name},
                couldn't resolve {pipeline_name}'''.format(
                    handle_pipeline_name=pipeline_def.name, pipeline_name=pipeline_name
                ),
            )
            return pipeline_def
        return self.get_handle().with_pipeline_name(pipeline_name).build_pipeline_definition()
