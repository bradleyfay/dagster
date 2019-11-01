import os
import pickle
import re
import tempfile
import time

import pytest

from dagster import (
    Any,
    Bool,
    DagsterInvalidConfigError,
    DagsterInvalidDefinitionError,
    Dict,
    Field,
    Float,
    InputDefinition,
    Int,
    List,
    Nothing,
    Optional,
    Path,
    Set,
    String,
    Tuple,
    execute_pipeline,
    execute_solid,
    pipeline,
    solid,
)


@solid
def identity(_, x: Any) -> Any:
    return x


@solid
def identity_imp(_, x):
    return x


@solid
def boolean(_, x: Bool) -> String:
    return 'true' if x else 'false'


@solid
def empty_string(_, x: String) -> bool:
    return len(x) == 0


@solid
def add_3(_, x: Int) -> int:
    return x + 3


@solid
def div_2(_, x: Float) -> float:
    return x / 2


@solid
def concat(_, x: String, y: str) -> str:
    return x + y


@solid
def exists(_, path: Path) -> Bool:
    return os.path.exists(path)


@solid
def wait(_) -> Nothing:
    time.sleep(1)
    return


@solid(input_defs=[InputDefinition('ready', dagster_type=Nothing)])
def done(_) -> str:
    return 'done'


@pipeline
def nothing_pipeline():
    done(wait())


@solid
def wait_int(_) -> Int:
    time.sleep(1)
    return 1


@pipeline
def nothing_int_pipeline():
    done(wait_int())


@solid
def nullable_concat(_, x: String, y: Optional[String]) -> String:
    return x + (y or '')


@solid
def concat_list(_, xs: List[String]) -> String:
    return ''.join(xs)


@solid
def emit_1(_) -> int:
    return 1


@solid
def emit_2(_) -> int:
    return 2


@solid
def emit_3(_) -> int:
    return 3


@solid
def sum_solid(_, xs: List[int]) -> int:
    return sum(xs)


@pipeline
def sum_pipeline():
    sum_solid([emit_1(), emit_2(), emit_3()])


@solid
def repeat(_, spec: Dict) -> str:
    return spec['word'] * spec['times']


@solid
def set_solid(_, set_input: Set[String]) -> List[String]:
    return sorted([x for x in set_input])


@solid
def tuple_solid(_, tuple_input: Tuple[String, Int, Float]) -> List:
    return [x for x in tuple_input]


def test_identity():
    res = execute_solid(identity, input_values={'x': 'foo'})
    assert res.output_value() == 'foo'


def test_identity_imp():
    res = execute_solid(identity_imp, input_values={'x': 'foo'})
    assert res.output_value() == 'foo'


def test_boolean():
    res = execute_solid(boolean, input_values={'x': True})
    assert res.output_value() == 'true'

    res = execute_solid(boolean, input_values={'x': False})
    assert res.output_value() == 'false'


def test_empty_string():
    res = execute_solid(empty_string, input_values={'x': ''})
    assert res.output_value() is True

    res = execute_solid(empty_string, input_values={'x': 'foo'})
    assert res.output_value() is False


def test_add_3():
    res = execute_solid(add_3, input_values={'x': 3})
    assert res.output_value() == 6


def test_div_2():
    res = execute_solid(div_2, input_values={'x': 7.0})
    assert res.output_value() == 3.5


def test_concat():
    res = execute_solid(concat, input_values={'x': 'foo', 'y': 'bar'})
    assert res.output_value() == 'foobar'


def test_exists():
    res = execute_solid(exists, input_values={'path': 'garkjgh.dkjhfk'})
    assert res.output_value() is False


def test_nothing_pipeline():
    res = execute_pipeline(nothing_pipeline)
    assert res.result_for_solid('wait').output_value() is None
    assert res.result_for_solid('done').output_value() == 'done'


def test_nothing_int_pipeline():
    res = execute_pipeline(nothing_int_pipeline)
    assert res.result_for_solid('wait_int').output_value() == 1
    assert res.result_for_solid('done').output_value() == 'done'


def test_nullable_concat():
    res = execute_solid(nullable_concat, input_values={'x': 'foo', 'y': None})
    assert res.output_value() == 'foo'


def test_concat_list():
    res = execute_solid(concat_list, input_values={'xs': ['foo', 'bar', 'baz']})
    assert res.output_value() == 'foobarbaz'


def test_sum_pipeline():
    res = execute_pipeline(sum_pipeline)
    assert res.result_for_solid('sum_solid').output_value() == 6


def test_repeat():
    res = execute_solid(repeat, input_values={'spec': {'word': 'foo', 'times': 3}})
    assert res.output_value() == 'foofoofoo'


def test_set_solid():
    res = execute_solid(set_solid, input_values={'set_input': {'foo', 'bar', 'baz'}})
    assert res.output_value() == sorted(['foo', 'bar', 'baz'])


def test_set_solid_configable_input():
    res = execute_solid(
        set_solid,
        environment_dict={
            'solids': {
                'set_solid': {
                    'inputs': {'set_input': [{'value': 'foo'}, {'value': 'bar'}, {'value': 'baz'}]}
                }
            }
        },
    )
    assert res.output_value() == sorted(['foo', 'bar', 'baz'])


def test_set_solid_configable_input_bad():
    with pytest.raises(
        DagsterInvalidConfigError,
        match=re.escape(
            'Type failure at path "root:solids:set_solid:inputs:set_input" on type '
            '"List[String]". Value at path root:solids:set_solid:inputs:set_input must be list. '
            'Expected: [{ json: { path: Path } pickle: { path: Path } value: String }].'
        ),
    ):
        execute_solid(
            set_solid,
            environment_dict={
                'solids': {'set_solid': {'inputs': {'set_input': {'foo', 'bar', 'baz'}}}}
            },
        )


def test_tuple_solid():
    res = execute_solid(tuple_solid, input_values={'tuple_input': ('foo', 1, 3.1)})
    assert res.output_value() == ['foo', 1, 3.1]


def test_tuple_solid_configable_input():
    res = execute_solid(
        tuple_solid,
        environment_dict={
            'solids': {
                'tuple_solid': {
                    'inputs': {'tuple_input': [{'value': 'foo'}, {'value': 1}, {'value': 3.1}]}
                }
            }
        },
    )
    assert res.output_value() == ['foo', 1, 3.1]


def test_tuple_solid_configable_input_bad():
    with pytest.raises(
        DagsterInvalidConfigError,
        match=re.escape(
            'Error 1: Type failure at path "root:solids:tuple_solid:inputs:tuple_input[0]". Value '
            'for selector type String.InputHydrationConfig must be a dict.'
        ),
    ):
        execute_solid(
            tuple_solid,
            environment_dict={
                'solids': {'tuple_solid': {'inputs': {'tuple_input': ['foo', 1, 3.1]}}}
            },
        )


######


@solid(config_field=Field(Any))
def any_config(context):
    return context.solid_config


@solid(config_field=Field(Bool))
def bool_config(context):
    return 'true' if context.solid_config else 'false'


@solid(config_field=Field(Int))
def add_n(context, x: Int) -> int:
    return x + context.solid_config


@solid(config_field=Field(Float))
def div_y(context, x: Float) -> float:
    return x / context.solid_config


@solid(config_field=Field(float))
def div_y_var(context, x: Float) -> float:
    return x / context.solid_config


@solid(config_field=Field(String))
def hello(context) -> str:
    return 'Hello, {friend}!'.format(friend=context.solid_config)


@solid(config_field=Field(Path))
def unpickle(context) -> Any:
    with open(context.solid_config, 'rb') as fd:
        return pickle.load(fd)


@solid(config_field=Field(List[String]))
def concat_config(context) -> String:
    return ''.join(context.solid_config)


@solid(config_field=Field(Dict({'word': Field(String), 'times': Field(Int)})))
def repeat_config(context) -> str:
    return context.solid_config['word'] * context.solid_config['times']


def test_any_config():
    res = execute_solid(any_config, environment_dict={'solids': {'any_config': {'config': 'foo'}}})
    assert res.output_value() == 'foo'

    res = execute_solid(
        any_config, environment_dict={'solids': {'any_config': {'config': {'zip': 'zowie'}}}}
    )
    assert res.output_value() == {'zip': 'zowie'}


def test_bool_config():
    res = execute_solid(bool_config, environment_dict={'solids': {'bool_config': {'config': True}}})
    assert res.output_value() == 'true'

    res = execute_solid(
        bool_config, environment_dict={'solids': {'bool_config': {'config': False}}}
    )
    assert res.output_value() == 'false'


def test_add_n():
    res = execute_solid(
        add_n, input_values={'x': 3}, environment_dict={'solids': {'add_n': {'config': 7}}}
    )
    assert res.output_value() == 10


def test_div_y():
    res = execute_solid(
        div_y, input_values={'x': 3.0}, environment_dict={'solids': {'div_y': {'config': 2.0}}}
    )
    assert res.output_value() == 1.5


def test_div_y_var():
    res = execute_solid(
        div_y_var,
        input_values={'x': 3.0},
        environment_dict={'solids': {'div_y_var': {'config': 2.0}}},
    )
    assert res.output_value() == 1.5


def test_hello():
    res = execute_solid(hello, environment_dict={'solids': {'hello': {'config': 'Max'}}})
    assert res.output_value() == 'Hello, Max!'


def test_unpickle():
    with tempfile.NamedTemporaryFile() as fd:
        pickle.dump('foo', fd)
        fd.seek(0)
        res = execute_solid(
            unpickle, environment_dict={'solids': {'unpickle': {'config': fd.name}}}
        )
        assert res.output_value() == 'foo'


def test_concat_config():
    res = execute_solid(
        concat_config,
        environment_dict={'solids': {'concat_config': {'config': ['foo', 'bar', 'baz']}}},
    )
    assert res.output_value() == 'foobarbaz'


def test_repeat_config():
    res = execute_solid(
        repeat_config,
        environment_dict={'solids': {'repeat_config': {'config': {'word': 'foo', 'times': 3}}}},
    )
    assert res.output_value() == 'foofoofoo'


def test_set_config():
    with pytest.raises(DagsterInvalidDefinitionError) as exc:

        @solid(config_field=Field(Set[String]))
        def _set_config_solid(context, words) -> List:
            return [word for word in words if word in context.solid_config]

    assert (
        'Attempted to pass typing.Set[String] to a Field that expects a valid dagster type usable '
        'in config'
    ) in str(exc.value)


def test_tuple_config():
    with pytest.raises(DagsterInvalidDefinitionError) as exc:

        @solid(config_field=Field(Tuple[String, Int, Float]))
        def _tuple_config_solid(context) -> List:
            return [x for x in context.solid_config]

    assert (
        'Attempted to pass typing.Tuple[String, Int, Float] to a Field that expects a valid '
        'dagster type usable in config'
    ) in str(exc.value)
