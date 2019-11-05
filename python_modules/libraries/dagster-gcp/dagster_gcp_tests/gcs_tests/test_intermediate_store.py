import uuid
from io import BytesIO

import pytest
from dagster_gcp.gcs.intermediate_store import GCSIntermediateStore
from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.gcs.system_storage import gcs_plus_default_storage_defs

from dagster import (
    Bool,
    InputDefinition,
    Int,
    List,
    ModeDefinition,
    OutputDefinition,
    RunConfig,
    SerializationStrategy,
    String,
    check,
    lambda_solid,
    pipeline,
)
from dagster.core.events import DagsterEventType
from dagster.core.execution.api import create_execution_plan, execute_plan, scoped_pipeline_context
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.core.storage.type_storage import TypeStoragePlugin, TypeStoragePluginRegistry
from dagster.core.types.runtime import Bool as RuntimeBool
from dagster.core.types.runtime import RuntimeType
from dagster.core.types.runtime import String as RuntimeString
from dagster.core.types.runtime import resolve_to_runtime_type
from dagster.utils.test import yield_empty_pipeline_context


class UppercaseSerializationStrategy(SerializationStrategy):  # pylint: disable=no-init
    def serialize(self, value, write_file_obj):
        return write_file_obj.write(bytes(value.upper().encode('utf-8')))

    def deserialize(self, read_file_obj):
        return read_file_obj.read().decode('utf-8').lower()


class LowercaseString(RuntimeType):
    def __init__(self):
        super(LowercaseString, self).__init__(
            'lowercase_string',
            'LowercaseString',
            serialization_strategy=UppercaseSerializationStrategy('uppercase'),
        )


nettest = pytest.mark.nettest


def define_inty_pipeline():
    @lambda_solid
    def return_one():
        return 1

    @lambda_solid(input_defs=[InputDefinition('num', Int)], output_def=OutputDefinition(Int))
    def add_one(num):
        return num + 1

    @lambda_solid
    def user_throw_exception():
        raise Exception('whoops')

    @pipeline(
        mode_defs=[
            ModeDefinition(
                system_storage_defs=gcs_plus_default_storage_defs,
                resource_defs={'gcs': gcs_resource},
            )
        ]
    )
    def basic_external_plan_execution():
        add_one(return_one())
        user_throw_exception()

    return basic_external_plan_execution


def get_step_output(step_events, step_key, output_name='result'):
    for step_event in step_events:
        if (
            step_event.event_type == DagsterEventType.STEP_OUTPUT
            and step_event.step_key == step_key
            and step_event.step_output_data.output_name == output_name
        ):
            return step_event
    return None


@nettest
def test_using_gcs_for_subplan(gcs_bucket):
    pipeline_def = define_inty_pipeline()

    environment_dict = {'storage': {'gcs': {'config': {'gcs_bucket': gcs_bucket}}}}

    run_id = str(uuid.uuid4())

    execution_plan = create_execution_plan(
        pipeline_def, environment_dict=environment_dict, run_config=RunConfig(run_id=run_id)
    )

    assert execution_plan.get_step_by_key('return_one.compute')

    step_keys = ['return_one.compute']
    instance = DagsterInstance.ephemeral()
    pipeline_run = PipelineRun.create_empty_run(
        pipeline_def.name, run_id=run_id, environment_dict=environment_dict
    )

    return_one_step_events = list(
        execute_plan(
            execution_plan,
            environment_dict=environment_dict,
            pipeline_run=pipeline_run,
            step_keys_to_execute=step_keys,
            instance=instance,
        )
    )

    assert get_step_output(return_one_step_events, 'return_one.compute')
    with scoped_pipeline_context(pipeline_def, environment_dict, pipeline_run, instance) as context:
        store = GCSIntermediateStore(
            gcs_bucket, run_id, client=context.scoped_resources_builder.build().gcs.client
        )
        assert store.has_intermediate(context, 'return_one.compute')
        assert store.get_intermediate(context, 'return_one.compute', Int).obj == 1

    add_one_step_events = list(
        execute_plan(
            execution_plan,
            environment_dict=environment_dict,
            pipeline_run=pipeline_run,
            step_keys_to_execute=['add_one.compute'],
            instance=instance,
        )
    )

    assert get_step_output(add_one_step_events, 'add_one.compute')
    with scoped_pipeline_context(pipeline_def, environment_dict, pipeline_run, instance) as context:
        assert store.has_intermediate(context, 'add_one.compute')
        assert store.get_intermediate(context, 'add_one.compute', Int).obj == 2


class FancyStringGCSTypeStoragePlugin(TypeStoragePlugin):  # pylint:disable=no-init
    @classmethod
    def compatible_with_storage_def(cls, _):
        # Not needed for these tests
        raise NotImplementedError()

    @classmethod
    def set_object(cls, intermediate_store, obj, context, runtime_type, paths):
        check.inst_param(intermediate_store, 'intermediate_store', GCSIntermediateStore)
        paths.append(obj)
        return intermediate_store.set_object('', context, runtime_type, paths)

    @classmethod
    def get_object(cls, intermediate_store, _context, _runtime_type, paths):
        check.inst_param(intermediate_store, 'intermediate_store', GCSIntermediateStore)
        res = list(
            intermediate_store.object_store.client.list_blobs(
                intermediate_store.object_store.bucket,
                prefix=intermediate_store.key_for_paths(paths),
            )
        )
        return res[0].name.split('/')[-1]


@nettest
def test_gcs_intermediate_store_with_type_storage_plugin(gcs_bucket):
    run_id = str(uuid.uuid4())

    intermediate_store = GCSIntermediateStore(
        run_id=run_id,
        gcs_bucket=gcs_bucket,
        type_storage_plugin_registry=TypeStoragePluginRegistry(
            {RuntimeString.inst(): FancyStringGCSTypeStoragePlugin}
        ),
    )

    with yield_empty_pipeline_context(run_id=run_id) as context:
        try:
            intermediate_store.set_value('hello', context, RuntimeString.inst(), ['obj_name'])

            assert intermediate_store.has_object(context, ['obj_name'])
            assert (
                intermediate_store.get_value(context, RuntimeString.inst(), ['obj_name']) == 'hello'
            )

        finally:
            intermediate_store.rm_object(context, ['obj_name'])


@nettest
def test_gcs_intermediate_store_with_composite_type_storage_plugin(gcs_bucket):
    run_id = str(uuid.uuid4())

    intermediate_store = GCSIntermediateStore(
        run_id=run_id,
        gcs_bucket=gcs_bucket,
        type_storage_plugin_registry=TypeStoragePluginRegistry(
            {RuntimeString.inst(): FancyStringGCSTypeStoragePlugin}
        ),
    )

    with yield_empty_pipeline_context(run_id=run_id) as context:
        with pytest.raises(check.NotImplementedCheckError):
            intermediate_store.set_value(
                ['hello'], context, resolve_to_runtime_type(List[String]), ['obj_name']
            )


@nettest
def test_gcs_intermediate_store_composite_types_with_custom_serializer_for_inner_type(gcs_bucket):
    run_id = str(uuid.uuid4())

    intermediate_store = GCSIntermediateStore(run_id=run_id, gcs_bucket=gcs_bucket)
    with yield_empty_pipeline_context(run_id=run_id) as context:
        try:
            intermediate_store.set_object(
                ['foo', 'bar'],
                context,
                resolve_to_runtime_type(List[LowercaseString]).inst(),
                ['list'],
            )
            assert intermediate_store.has_object(context, ['list'])
            assert intermediate_store.get_object(
                context, resolve_to_runtime_type(List[Bool]).inst(), ['list']
            ).obj == ['foo', 'bar']

        finally:
            intermediate_store.rm_object(context, ['foo'])


@nettest
def test_gcs_intermediate_store_with_custom_serializer(gcs_bucket):
    run_id = str(uuid.uuid4())

    intermediate_store = GCSIntermediateStore(run_id=run_id, gcs_bucket=gcs_bucket)

    with yield_empty_pipeline_context(run_id=run_id) as context:
        try:
            intermediate_store.set_object('foo', context, LowercaseString.inst(), ['foo'])

            bucket_obj = intermediate_store.object_store.client.get_bucket(
                intermediate_store.object_store.bucket
            )
            blob = bucket_obj.blob('/'.join([intermediate_store.root] + ['foo']))
            file_obj = BytesIO()
            blob.download_to_file(file_obj)
            file_obj.seek(0)

            assert file_obj.read().decode('utf-8') == 'FOO'

            assert intermediate_store.has_object(context, ['foo'])
            assert (
                intermediate_store.get_object(context, LowercaseString.inst(), ['foo']).obj == 'foo'
            )
        finally:
            intermediate_store.rm_object(context, ['foo'])


@nettest
def test_gcs_intermediate_store(gcs_bucket):
    run_id = str(uuid.uuid4())
    run_id_2 = str(uuid.uuid4())

    intermediate_store = GCSIntermediateStore(run_id=run_id, gcs_bucket=gcs_bucket)
    assert intermediate_store.root == '/'.join(['dagster', 'storage', run_id])

    intermediate_store_2 = GCSIntermediateStore(run_id=run_id_2, gcs_bucket=gcs_bucket)
    assert intermediate_store_2.root == '/'.join(['dagster', 'storage', run_id_2])

    try:
        with yield_empty_pipeline_context(run_id=run_id) as context:

            intermediate_store.set_object(True, context, RuntimeBool.inst(), ['true'])

            assert intermediate_store.has_object(context, ['true'])
            assert intermediate_store.get_object(context, RuntimeBool.inst(), ['true']).obj is True
            assert intermediate_store.uri_for_paths(['true']).startswith('gs://')

            intermediate_store_2.copy_object_from_prev_run(context, run_id, ['true'])
            assert intermediate_store_2.has_object(context, ['true'])
            assert (
                intermediate_store_2.get_object(context, RuntimeBool.inst(), ['true']).obj is True
            )
    finally:
        intermediate_store.rm_object(context, ['true'])
        intermediate_store_2.rm_object(context, ['true'])