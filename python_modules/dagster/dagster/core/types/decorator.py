from dagster import check

from .mapping import register_python_type
from .runtime import define_python_dagster_type


def _decorate_as_dagster_type(
    bare_cls,
    name,
    description,
    input_hydration_config=None,
    output_materialization_config=None,
    serialization_strategy=None,
    auto_plugins=None,
    typecheck_metadata_fn=None,
    type_check=None,
):
    dagster_type_cls = define_python_dagster_type(
        name=name,
        description=description,
        python_type=bare_cls,
        input_hydration_config=input_hydration_config,
        output_materialization_config=output_materialization_config,
        serialization_strategy=serialization_strategy,
        auto_plugins=auto_plugins,
        typecheck_metadata_fn=typecheck_metadata_fn,
        type_check=type_check,
    )

    register_python_type(bare_cls, dagster_type_cls)

    return bare_cls


def dagster_type(
    name=None,
    description=None,
    input_hydration_config=None,
    output_materialization_config=None,
    serialization_strategy=None,
    auto_plugins=None,
    typecheck_metadata_fn=None,
    type_check=None,
):
    '''
    Decorator version of as_dagster_type.
    
    This allows for the straightforward creation of your own classes for use in your
    business logic, and then annotating them to make those same classes compatible with
    the dagster type system.

    e.g.:
        .. code-block:: python

            # You have created an object for your own purposes within your app
            class MyDataObject:
                pass


    Now you want to be able to mark this as an input or output to solid. Without
    modification, this does not work.

    You must decorate it:
        .. code-block:: python

            @dagster_type
            class MyDataObject:
                pass

    Now one can using this as an input or an output into a solid.

        .. code-block:: python

            @lambda_solid
            def create_myobject() -> MyDataObject:
                return MyDataObject()

    And it is viewable in dagit and so forth, and you can use the dagster type system
    for configuration, serialization, and metadata emission.

    Args:
        python_type (cls): The python type to wrap as a Dagster type.
        name (Optional[str]): Name of the new Dagster type. If None, the name (__name__) of the
            python_type will be used. Default: None
        description (Optional[str]): A user-readable description of the type. Default: None.
        input_hydration_config (Optional[InputHydrationConfig]): An instance of a class that
            inherits from :py:class:`InputHydrationConfig <dagster.InputHydrationConfig>` and can
            map config data to a value of this type. Specify this argument if you will need to shim
            values of this type using the config machinery. As a rule, you should use the
            :py:func:`@input_hydration_config <dagster.InputHydrationConfig>` decorator to construct
            these arguments. Default: None
        output_materialization_config (Optiona[OutputMaterializationConfig]): An instance of a class
            that inherits from
            :py:class:`OutputMaterializationConfig <dagster.OutputMaterializationConfig>` that can
            persist values of this type. As a rule, you should use the
            :py:func:`@output_materialization_config <dagster.output_materialization_config>`
            decorator to construct these arguments. Default: None
        serialization_strategy (Optional[SerializationStrategy]): An instance of a class that
            inherits from :py:class:`SerializationStrategy <dagster.SerializationStrategy>`. The
            default strategy for serializing this value when automatically persisting it between
            execution steps. You should set this value if the ordinary serialization machinery
            (e.g., pickle) will not be adequate for this type. Default: None.
        auto_plugins (Optional[List[TypeStoragePlugin]]): If types must be serialized differently
            depending on the storage being used for intermediates, they should specify this
            argument. In these cases the serialization_strategy argument is not sufficient because
            serialization requires specialized API calls, e.g. to call an s3 API directly instead
            of using a generic file object. See dagster_pyspark.DataFrame for an example using
            auto_plugins. Default: None.
        typecheck_metadata_fn (Optional[Callable[[Any], TypeCheck]]): If specified, this function
            wil be called to emit metadata when you successfully check a type. The
            typecheck_metadata_fn will be passed the value being type-checked and should return an
            instance of :py:class:`TypeCheck <dagster.TypeCheck>`. See dagster_pandas.DataFrame for
            an example. Default: None.
        type_check (Optional[Callable[[Any], Any]]): If specified, this function will be called in
            place of the default isinstance type check. This function should raise Failure if the
            type check fails, and otherwise pass. Its return value will be ignored.
    '''

    def _with_args(bare_cls):
        check.type_param(bare_cls, 'bare_cls')
        new_name = name if name else bare_cls.__name__
        return _decorate_as_dagster_type(
            bare_cls=bare_cls,
            name=new_name,
            description=description,
            input_hydration_config=input_hydration_config,
            output_materialization_config=output_materialization_config,
            serialization_strategy=serialization_strategy,
            auto_plugins=auto_plugins,
            typecheck_metadata_fn=typecheck_metadata_fn,
            type_check=type_check,
        )

    # check for no args, no parens case
    if callable(name):
        klass = name  # with no parens, name is actually the decorated class
        return _decorate_as_dagster_type(bare_cls=klass, name=klass.__name__, description=None)

    return _with_args


def as_dagster_type(
    existing_type,
    name=None,
    description=None,
    input_hydration_config=None,
    output_materialization_config=None,
    serialization_strategy=None,
    auto_plugins=None,
    typecheck_metadata_fn=None,
    type_check=None,
):
    '''
    See documentation for :py:func:`define_python_dagster_type` for parameters.

    Takes a python cls and creates a type for it in the Dagster domain.

    Frequently you want to import a data processing library and use its types
    directly in solid definitions. To support this dagster has this facility
    that allows one to annotate *existing* classes as dagster type.

    from existing_library import FancyDataType as ExistingFancyDataType

    FancyDataType = as_dagster_type(existing_type=ExistingFancyDataType, name='FancyDataType')

    While one *could* use the existing type directly from the original library, we would
    recommend using the object returned by as_dagster_type to avoid an import-order-based bugs.

    See dagster_pandas for an example of how to do this.

    Args:
        python_type (cls): The python type to wrap as a Dagster type.
        name (Optional[str]): Name of the new Dagster type. If None, the name (__name__) of the
            python_type will be used. Default: None
        description (Optional[str]): A user-readable description of the type. Default: None.
        input_hydration_config (Optional[InputHydrationConfig]): An instance of a class that
            inherits from :py:class:`InputHydrationConfig <dagster.InputHydrationConfig>` and can
            map config data to a value of this type. Specify this argument if you will need to shim
            values of this type using the config machinery. As a rule, you should use the
            :py:func:`@input_hydration_config <dagster.InputHydrationConfig>` decorator to construct
            these arguments. Default: None
        output_materialization_config (Optiona[OutputMaterializationConfig]): An instance of a class
            that inherits from
            :py:class:`OutputMaterializationConfig <dagster.OutputMaterializationConfig>` that can
            persist values of this type. As a rule, you should use the
            :py:func:`@output_materialization_config <dagster.output_materialization_config>`
            decorator to construct these arguments. Default: None
        serialization_strategy (Optional[SerializationStrategy]): An instance of a class that
            inherits from :py:class:`SerializationStrategy <dagster.SerializationStrategy>`. The
            default strategy for serializing this value when automatically persisting it between
            execution steps. You should set this value if the ordinary serialization machinery
            (e.g., pickle) will not be adequate for this type. Default: None.
        auto_plugins (Optional[List[TypeStoragePlugin]]): If types must be serialized differently
            depending on the storage being used for intermediates, they should specify this
            argument. In these cases the serialization_strategy argument is not sufficient because
            serialization requires specialized API calls, e.g. to call an s3 API directly instead
            of using a generic file object. See dagster_pyspark.DataFrame for an example using
            auto_plugins. Default: None.
        typecheck_metadata_fn (Optional[Callable[[Any], TypeCheck]]): If specified, this function
            wil be called to emit metadata when you successfully check a type. The
            typecheck_metadata_fn will be passed the value being type-checked and should return an
            instance of :py:class:`TypeCheck <dagster.TypeCheck>`. See dagster_pandas.DataFrame for
            an example. Default: None.
        type_check (Optional[Callable[[Any], Any]]): If specified, this function will be called in
            place of the default isinstance type check. This function should raise Failure if the
            type check fails, and otherwise pass. Its return value will be ignored.
    '''

    return _decorate_as_dagster_type(
        bare_cls=check.type_param(existing_type, 'existing_type'),
        name=check.opt_str_param(name, 'name', existing_type.__name__),
        description=description,
        input_hydration_config=input_hydration_config,
        output_materialization_config=output_materialization_config,
        serialization_strategy=serialization_strategy,
        auto_plugins=auto_plugins,
        typecheck_metadata_fn=typecheck_metadata_fn,
        type_check=type_check,
    )
