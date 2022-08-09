import warnings

import pytest

from dagster import DependencyDefinition, In, Int, Nothing, OpDefinition, Out, Output, op
from dagster._core.definitions.decorators.hook_decorator import event_list_hook, success_hook
from dagster._core.definitions.events import DynamicOutput, HookExecutionResult
from dagster._core.errors import DagsterInvalidDefinitionError, DagsterInvariantViolationError
from dagster._core.execution.api import create_execution_plan
from dagster._legacy import (
    InputDefinition,
    OutputDefinition,
    PipelineDefinition,
    composite_solid,
    execute_pipeline,
    pipeline,
)


def builder(graph):
    return graph.add_one(graph.return_one())


@op(out=Out(Int))
def echo(blah):
    return blah


@op
def return_one():
    return 1


@op
def return_two():
    return 2


@op
def return_tuple():
    return (1, 2)


@op(ins={"num": In()})
def add_one(num):
    return num + 1


@op(ins={"num": In()})
def pipe(num):
    return num


@op(
    ins={"int_1": In(Int), "int_2": In(Int)},
    out=Out(Int),
)
def adder(_context, int_1, int_2):
    return int_1 + int_2


@op(
    out={
        "one": Out(
            Int,
        ),
        "two": Out(
            Int,
        ),
    }
)
def return_mult(_context):
    yield Output(1, "one")
    yield Output(2, "two")


@op(config_schema=int)
def return_config_int(context):
    return context.op_config


def get_duplicate_ops():
    return (
        OpDefinition(name="a_op", ins={}, compute_fn=lambda: None, outs={}),
        OpDefinition(name="a_op", ins={}, compute_fn=lambda: None, outs={}),
    )


def test_basic():
    @composite_solid
    def test():
        one = return_one()
        add_one(num=one)

    assert (
        execute_pipeline(PipelineDefinition(solid_defs=[test], name="test"))
        .result_for_handle("test.add_one")
        .output_value()
        == 2
    )


def test_args():
    @composite_solid
    def _test_1():
        one = return_one()
        add_one(one)

    @composite_solid
    def _test_2():
        adder(return_one(), return_two())

    @composite_solid
    def _test_3():
        adder(int_1=return_one(), int_2=return_two())

    @composite_solid
    def _test_4():
        adder(return_one(), return_two())

    @composite_solid
    def _test_5():
        adder(return_one(), int_2=return_two())

    @composite_solid
    def _test_6():
        adder(return_one())

    @composite_solid
    def _test_7():
        adder(int_2=return_two())


def test_arg_fails():

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _fail_2():
            adder(return_one(), 1)

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _fail_3():
            # pylint: disable=too-many-function-args
            adder(return_one(), return_two(), return_one.alias("three")())


def test_mult_out_fail():

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _test():
            ret = return_mult()
            add_one(ret)


def test_aliased_with_name_name_fails():
    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _test():
            one, two = return_mult()
            add_one(num=one)
            add_one.alias("add_one")(num=two)  # explicit alias disables autoalias


def test_composite_with_duplicate_ops():
    op_1, op_2 = get_duplicate_ops()
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="Detected conflicting node definitions with the same name",
    ):

        @composite_solid
        def _name_conflict_composite():
            op_1()
            op_2()


def test_pipeline_with_duplicate_ops():
    op_1, op_2 = get_duplicate_ops()
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="Detected conflicting node definitions with the same name",
    ):

        @pipeline
        def _name_conflict_pipeline():
            op_1()
            op_2()


def test_multiple():
    @composite_solid
    def test():
        one, two = return_mult()
        add_one(num=one)
        add_one.alias("add_one_2")(num=two)

    results = execute_pipeline(PipelineDefinition(solid_defs=[test], name="test"))
    assert results.result_for_handle("test.add_one").output_value() == 2
    assert results.result_for_handle("test.add_one_2").output_value() == 3


def test_two_inputs_with_dsl():
    @op(ins={"num_one": In(), "num_two": In()})
    def subtract(num_one, num_two):
        return num_one - num_two

    @op
    def return_three():
        return 3

    @composite_solid
    def test():
        subtract(num_one=return_two(), num_two=return_three())

    assert (
        execute_pipeline(PipelineDefinition(solid_defs=[test], name="test"))
        .result_for_handle("test.subtract")
        .output_value()
        == -1
    )


def test_basic_aliasing_with_dsl():
    @composite_solid
    def test():
        add_one.alias("renamed")(num=return_one())

    assert (
        execute_pipeline(PipelineDefinition(solid_defs=[test], name="test"))
        .result_for_handle("test.renamed")
        .output_value()
        == 2
    )


def test_diamond_graph():
    @op(out={"value_one": Out(), "value_two": Out()})
    def emit_values(_context):
        yield Output(1, "value_one")
        yield Output(2, "value_two")

    @op(ins={"num_one": In(), "num_two": In()})
    def subtract(num_one, num_two):
        return num_one - num_two

    @composite_solid
    def diamond():
        value_one, value_two = emit_values()
        subtract(
            num_one=add_one(num=value_one),
            num_two=add_one.alias("renamed")(num=value_two),
        )

    result = execute_pipeline(PipelineDefinition(solid_defs=[diamond], name="test"))

    assert result.result_for_handle("diamond.subtract").output_value() == -1


def test_mapping():
    @op(
        ins={"num_in": In(Int)},
        out={
            "num_out": Out(
                Int,
            )
        },
    )
    def double(num_in):
        return num_in * 2

    @composite_solid(
        input_defs=[InputDefinition("num_in", Int)],
        output_defs=[OutputDefinition(Int, "num_out")],
    )
    def composed_inout(num_in):
        return double(num_in=num_in)

    # have to use "pipe" op since "result_for_solid" doesnt work with composite mappings
    assert (
        execute_pipeline(
            PipelineDefinition(
                solid_defs=[return_one, composed_inout, pipe],
                name="test",
                dependencies={
                    "composed_inout": {"num_in": DependencyDefinition("return_one")},
                    "pipe": {"num": DependencyDefinition("composed_inout", "num_out")},
                },
            )
        )
        .result_for_solid("pipe")
        .output_value()
        == 2
    )


def test_mapping_args_kwargs():
    @op
    def take(a, b, c):
        return (a, b, c)

    @composite_solid
    def maps(m_c, m_b, m_a):
        take(m_a, b=m_b, c=m_c)

    assert maps.input_mappings[2].definition.name == "m_a"
    assert maps.input_mappings[2].maps_to.input_name == "a"

    assert maps.input_mappings[1].definition.name == "m_b"
    assert maps.input_mappings[1].maps_to.input_name == "b"

    assert maps.input_mappings[0].definition.name == "m_c"
    assert maps.input_mappings[0].maps_to.input_name == "c"


def test_output_map_mult():
    @composite_solid(output_defs=[OutputDefinition(Int, "one"), OutputDefinition(Int, "two")])
    def wrap_mult():
        return return_mult()

    @pipeline
    def mult_pipe():
        one, two = wrap_mult()
        echo.alias("echo_one")(one)
        echo.alias("echo_two")(two)

    result = execute_pipeline(mult_pipe)
    assert result.result_for_solid("echo_one").output_value() == 1
    assert result.result_for_solid("echo_two").output_value() == 2


def test_output_map_mult_swizzle():
    @composite_solid(output_defs=[OutputDefinition(Int, "x"), OutputDefinition(Int, "y")])
    def wrap_mult():
        one, two = return_mult()
        return {"x": one, "y": two}

    @pipeline
    def mult_pipe():
        x, y = wrap_mult()
        echo.alias("echo_x")(x)
        echo.alias("echo_y")(y)

    result = execute_pipeline(mult_pipe)
    assert result.result_for_solid("echo_x").output_value() == 1
    assert result.result_for_solid("echo_y").output_value() == 2


def test_output_map_fail():
    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid(output_defs=[OutputDefinition(Int, "one"), OutputDefinition(Int, "two")])
        def _bad(_context):
            return return_one()

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid(output_defs=[OutputDefinition(Int, "one"), OutputDefinition(Int, "two")])
        def _bad(_context):
            return {"one": 1}

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid(
            output_defs=[OutputDefinition(Int, "three"), OutputDefinition(Int, "four")]
        )
        def _bad():
            return return_mult()


def test_deep_graph():
    @op(config_schema=Int)
    def download_num(context):
        return context.op_config

    @op(ins={"num": In()})
    def unzip_num(num):
        return num

    @op(ins={"num": In()})
    def ingest_num(num):
        return num

    @op(ins={"num": In()})
    def subsample_num(num):
        return num

    @op(ins={"num": In()})
    def canonicalize_num(num):
        return num

    @op(ins={"num": In()}, out=Out(Int))
    def load_num(num):
        return num + 3

    @composite_solid(output_defs=[OutputDefinition(Int)])
    def test():
        return load_num(
            num=canonicalize_num(
                num=subsample_num(num=ingest_num(num=unzip_num(num=download_num())))
            )
        )

    result = execute_pipeline(
        PipelineDefinition(solid_defs=[test], name="test"),
        {"ops": {"test": {"ops": {"download_num": {"config": 123}}}}},
    )
    assert result.result_for_handle("test.canonicalize_num").output_value() == 123
    assert result.result_for_handle("test.load_num").output_value() == 126


def test_recursion():
    @composite_solid
    def outer():
        @composite_solid(output_defs=[OutputDefinition()])
        def inner():
            return add_one(return_one())

        add_one(inner())

    assert execute_pipeline(PipelineDefinition(solid_defs=[outer], name="test")).success


class Garbage(Exception):
    pass


def test_recursion_with_exceptions():
    called = {}

    @pipeline
    def recurse():
        @composite_solid
        def outer():
            try:

                @composite_solid
                def throws():
                    called["throws"] = True
                    raise Garbage()

                throws()
            except Garbage:
                add_one(return_one())

        outer()

    assert execute_pipeline(recurse).success
    assert called["throws"] is True


def test_pipeline_has_solid_def():
    @composite_solid(output_defs=[OutputDefinition()])
    def inner():
        return add_one(return_one())

    @composite_solid
    def outer():
        add_one(inner())

    @pipeline
    def a_pipeline():
        outer()

    assert a_pipeline.has_solid_def("add_one")
    assert a_pipeline.has_solid_def("outer")
    assert a_pipeline.has_solid_def("inner")


def test_mapping_args_ordering():
    @op
    def take(a, b, c):
        assert a == "a"
        assert b == "b"
        assert c == "c"

    @composite_solid
    def swizzle(b, a, c):
        take(a, b, c)

    @composite_solid
    def swizzle_2(c, b, a):
        swizzle(b, a=a, c=c)

    @pipeline
    def ordered():
        swizzle_2()

    for mapping in swizzle.input_mappings:
        assert mapping.definition.name == mapping.maps_to.input_name

    for mapping in swizzle_2.input_mappings:
        assert mapping.definition.name == mapping.maps_to.input_name

    execute_pipeline(
        ordered,
        {
            "ops": {
                "swizzle_2": {
                    "inputs": {
                        "a": {"value": "a"},
                        "b": {"value": "b"},
                        "c": {"value": "c"},
                    }
                }
            }
        },
    )


def test_unused_mapping():
    with pytest.raises(DagsterInvalidDefinitionError, match="unmapped input"):

        @composite_solid
        def unused_mapping(_):
            return_one()


@op
def single_input_op():
    return


def test_collision_invocations():
    with pytest.warns(None) as record:

        @pipeline
        def _():
            single_input_op()
            single_input_op()
            single_input_op()

    assert len(record) == 0


def test_alias_invoked(recwarn):
    @pipeline
    def _():
        single_input_op.alias("foo")()
        single_input_op.alias("bar")()

    assert len(recwarn) == 0


def test_alias_not_invoked():

    with pytest.warns(
        UserWarning,
        match=(
            r"While in @pipeline context '_my_pipeline', received an uninvoked op "
            r"'single_input_op'\.\n'single_input_op' was aliased as '(foo|bar)'."
        ),
    ) as record:

        @pipeline
        def _my_pipeline():
            single_input_op.alias("foo")
            single_input_op.alias("bar")

    assert len(record) == 2  # This pipeline should raise a warning for each aliasing of the op.


def test_tag_invoked():

    # See: https://docs.pytest.org/en/7.0.x/how-to/capture-warnings.html#additional-use-cases-of-warnings-in-tests
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=UserWarning)

        @pipeline
        def _my_pipeline():
            single_input_op.tag({})()

        execute_pipeline(_my_pipeline)


def test_tag_not_invoked():

    with pytest.warns(
        UserWarning,
        match=(
            r"While in @pipeline context '_my_pipeline', received an uninvoked op "
            r"'single_input_op'\."
        ),
    ) as record:

        @pipeline
        def _my_pipeline():
            single_input_op.tag({})
            single_input_op.tag({})

        execute_pipeline(_my_pipeline)

    user_warnings = [warning for warning in record if isinstance(warning.message, UserWarning)]
    assert len(user_warnings) == 1  # We should only raise one warning because ops have same name.

    with pytest.warns(
        UserWarning,
        match=(
            r"While in @pipeline context '_my_pipeline', received an uninvoked op "
            r"'single_input_op'\.\nProvided tags: {'a': 'b'}\."
        ),
    ):

        @pipeline
        def _my_pipeline():
            single_input_op.tag({"a": "b"})

        execute_pipeline(_my_pipeline)


def test_with_hooks_invoked():

    with warnings.catch_warnings():
        warnings.simplefilter("error", category=UserWarning)

        @pipeline
        def _my_pipeline():
            single_input_op.with_hooks(set())()

        execute_pipeline(_my_pipeline)


@event_list_hook(required_resource_keys=set())
def a_hook(_context, _):
    return HookExecutionResult("a_hook")


def test_with_hooks_not_invoked():
    with pytest.warns(
        UserWarning,
        match=(
            r"While in @pipeline context '_my_pipeline', received an uninvoked op "
            r"'single_input_op'\."
        ),
    ) as record:

        @pipeline
        def _my_pipeline():
            single_input_op.with_hooks(set())
            single_input_op.with_hooks(set())

        execute_pipeline(_my_pipeline)

    # Note not returning out of the pipe causes warning count to go up to 2
    user_warnings = [warning for warning in record if isinstance(warning.message, UserWarning)]
    assert len(user_warnings) == 1  # We should only raise one warning because ops have same name.

    with pytest.warns(
        UserWarning,
        match=(
            r"While in @pipeline context '_my_pipeline', received an uninvoked op "
            r"'single_input_op'\.\nProvided hook definitions: \['a_hook'\]\."
        ),
    ):

        @pipeline
        def _my_pipeline():
            single_input_op.with_hooks({a_hook})

        execute_pipeline(_my_pipeline)


def test_with_hooks_not_empty():
    @pipeline
    def _():
        single_input_op.with_hooks({a_hook})

    assert 1 == 1


def test_multiple_pending_invocations():
    with pytest.warns(
        UserWarning,
        match=(
            r"While in @pipeline context '_my_pipeline', received an uninvoked op "
            r"'single_input_op'\.\n'single_input_op' was aliased as 'bar'\.\n"
            r"Provided hook definitions: \['a_hook'\]\."
        ),
    ) as record:

        @pipeline
        def _my_pipeline():
            foo = single_input_op.alias("foo")
            bar = single_input_op.alias("bar")
            foo_tag = foo.tag({})
            _bar_hook = bar.with_hooks({a_hook})
            foo_tag()

    assert (
        len(record) == 1
    )  # ensure that one warning is thrown per op_name / alias instead of per every PendingNodeInvocation.


def test_compose_nothing():
    @op(ins={"start": In(Nothing)})
    def go():
        pass

    @composite_solid(input_defs=[InputDefinition("start", Nothing)])
    def _compose(start):
        go(start)  # pylint: disable=too-many-function-args


def test_multimap():
    @composite_solid(output_defs=[OutputDefinition(int, "x"), OutputDefinition(int, "y")])
    def multimap(foo):
        x = echo.alias("echo_1")(foo)
        y = echo.alias("echo_2")(foo)
        return {"x": x, "y": y}

    @pipeline
    def multimap_pipe():
        one = return_one()
        multimap(one)

    result = execute_pipeline(multimap_pipe)
    assert result.result_for_handle("multimap.echo_1").output_value() == 1
    assert result.result_for_handle("multimap.echo_2").output_value() == 1


def test_reuse_inputs():
    @composite_solid(input_defs=[InputDefinition("one", Int), InputDefinition("two", Int)])
    def calculate(one, two):
        adder(one, two)
        adder.alias("adder_2")(one, two)

    @pipeline
    def calculate_pipeline():
        one = return_one()
        two = return_two()
        calculate(one, two)

    result = execute_pipeline(calculate_pipeline)
    assert result.result_for_handle("calculate.adder").output_value() == 3
    assert result.result_for_handle("calculate.adder_2").output_value() == 3


def test_output_node_error():
    with pytest.raises(DagsterInvariantViolationError):

        @pipeline
        def _bad_destructure():
            _a, _b = return_tuple()

    with pytest.raises(DagsterInvariantViolationError):

        @pipeline
        def _bad_index():
            out = return_tuple()
            add_one(out[0])


def test_pipeline_composition_metadata():
    @op
    def metadata_op(context):
        return context.op.tags["key"]

    @pipeline
    def metadata_test_pipeline():
        metadata_op.tag({"key": "foo"}).alias("aliased_one")()
        metadata_op.alias("aliased_two").tag({"key": "foo"}).tag({"key": "bar"})()
        metadata_op.alias("aliased_three").tag({"key": "baz"})()
        metadata_op.tag({"key": "quux"})()

    res = execute_pipeline(metadata_test_pipeline)

    assert res.result_for_solid("aliased_one").output_value() == "foo"
    assert res.result_for_solid("aliased_two").output_value() == "bar"
    assert res.result_for_solid("aliased_three").output_value() == "baz"
    assert res.result_for_solid("metadata_op").output_value() == "quux"


def test_composite_solid_composition_metadata():
    @op
    def metadata_op(context):
        return context.op.tags["key"]

    @composite_solid
    def metadata_composite():
        metadata_op.tag({"key": "foo"}).alias("aliased_one")()
        metadata_op.alias("aliased_two").tag({"key": "foo"}).tag({"key": "bar"})()
        metadata_op.alias("aliased_three").tag({"key": "baz"})()
        metadata_op.tag({"key": "quux"})()

    @pipeline
    def metadata_test_pipeline():
        metadata_composite()

    res = execute_pipeline(metadata_test_pipeline)

    assert (
        res.result_for_solid("metadata_composite").result_for_solid("aliased_one").output_value()
        == "foo"
    )
    assert (
        res.result_for_solid("metadata_composite").result_for_solid("aliased_two").output_value()
        == "bar"
    )
    assert (
        res.result_for_solid("metadata_composite").result_for_solid("aliased_three").output_value()
        == "baz"
    )
    assert (
        res.result_for_solid("metadata_composite").result_for_solid("metadata_op").output_value()
        == "quux"
    )


def test_uninvoked_op_fails():
    with pytest.raises(DagsterInvalidDefinitionError, match=r".*Did you forget parentheses?"):

        @pipeline
        def uninvoked_op_pipeline():
            add_one(return_one)

        execute_pipeline(uninvoked_op_pipeline)


def test_uninvoked_aliased_op_fails():
    with pytest.raises(DagsterInvalidDefinitionError, match=r".*Did you forget parentheses?"):

        @pipeline
        def uninvoked_aliased_op_pipeline():
            add_one(return_one.alias("something"))

        execute_pipeline(uninvoked_aliased_op_pipeline)


def test_alias_on_invoked_op_fails():
    with pytest.raises(
        DagsterInvariantViolationError,
        match=r".*Consider checking the location of parentheses.",
    ):

        @pipeline
        def alias_on_invoked_op_pipeline():
            return_one().alias("something")  # pylint: disable=no-member

        execute_pipeline(alias_on_invoked_op_pipeline)


def test_warn_on_pipeline_return():
    @op
    def noop(_):
        pass

    with pytest.warns(
        UserWarning,
        match="You have returned a value out of a @pipeline-decorated function. ",
    ):

        @pipeline
        def _returns_something():
            return noop()


def test_tags():
    @op(tags={"def": "1"})
    def emit(_):
        return 1

    @pipeline
    def tag():
        emit.tag({"invoke": "2"})()

    plan = create_execution_plan(tag)
    step = list(plan.step_dict.values())[0]
    assert step.tags == {"def": "1", "invoke": "2"}


def test_bad_alias():
    with pytest.raises(DagsterInvalidDefinitionError, match="not a valid name"):
        echo.alias("uh oh")

    with pytest.raises(DagsterInvalidDefinitionError, match="not a valid name"):
        echo.alias("uh[oh]")


def test_tag_subset():
    @op
    def empty(_):
        pass

    @op(tags={"def": "1"})
    def emit(_):
        return 1

    @pipeline
    def tag():
        empty()
        emit.tag({"invoke": "2"})()

    plan = create_execution_plan(tag.get_pipeline_subset_def({"emit"}))
    step = list(plan.step_dict.values())[0]
    assert step.tags == {"def": "1", "invoke": "2"}


def test_composition_order():
    op_to_tags = {}

    @success_hook
    def test_hook(context):
        op_to_tags[context.op.name] = context.op.tags

    @op
    def a_op(_):
        pass

    @pipeline
    def a_pipeline():
        a_op.with_hooks(hook_defs={test_hook}).alias("hook_alias_tag").tag({"pos": 3})()
        a_op.with_hooks(hook_defs={test_hook}).tag({"pos": 2}).alias("hook_tag_alias")()
        a_op.alias("alias_tag_hook").tag({"pos": 2}).with_hooks(hook_defs={test_hook})()
        a_op.alias("alias_hook_tag").with_hooks(hook_defs={test_hook}).tag({"pos": 3})()
        a_op.tag({"pos": 1}).with_hooks(hook_defs={test_hook}).alias("tag_hook_alias")()
        a_op.tag({"pos": 1}).alias("tag_alias_hook").with_hooks(hook_defs={test_hook})()

    result = execute_pipeline(a_pipeline, raise_on_error=False)
    assert result.success
    assert op_to_tags == {
        "tag_hook_alias": {"pos": "1"},
        "tag_alias_hook": {"pos": "1"},
        "hook_tag_alias": {"pos": "2"},
        "alias_tag_hook": {"pos": "2"},
        "hook_alias_tag": {"pos": "3"},
        "alias_hook_tag": {"pos": "3"},
    }


def test_fan_in_scalars_fails():
    @op
    def fan_in_op(_, xs):
        return sum(xs)

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="Lists can only contain the output from previous op invocations or input mappings",
    ):

        @pipeline
        def _scalar_fan_in_pipeline():
            fan_in_op([1, 2, 3])


def test_with_hooks_on_invoked_op_fails():
    @op
    def yield_1_op(_):
        return 1

    with pytest.raises(
        DagsterInvariantViolationError,
        match="attempted to call hook method for InvokedSolidOutputHandle.",
    ):

        @pipeline
        def _bad_hooks_pipeline():
            yield_1_op().with_hooks({a_hook})


def test_iterating_over_dynamic_outputs_fails():
    @op
    def dynamic_output_op(_):
        yield DynamicOutput(1, "1")
        yield DynamicOutput(2, "2")

    @op
    def yield_input(_, x):
        return x

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Attempted to iterate over an InvokedSolidOutputHandle.",
    ):

        @pipeline
        def _iterating_over_dynamic_output_pipeline():
            for x in dynamic_output_op():
                yield_input(x)


def test_indexing_into_dynamic_outputs_fails():
    @op
    def dynamic_output_op(_):
        yield DynamicOutput(1, "1")
        yield DynamicOutput(2, "2")

    @op
    def yield_input(_, x):
        return x

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Attempted to index in to an InvokedSolidOutputHandle.",
    ):

        @pipeline
        def _indexing_into_dynamic_output_pipeline():
            yield_input(dynamic_output_op()[0])


def test_aliasing_invoked_dynamic_output_fails():
    @op
    def dynamic_output_op(_):
        yield DynamicOutput(1, "1")
        yield DynamicOutput(2, "2")

    with pytest.raises(
        DagsterInvariantViolationError,
        match="attempted to call alias method for InvokedSolidOutputHandle.",
    ):

        @pipeline
        def _alias_invoked_dynamic_output_pipeline():
            dynamic_output_op().alias("dynamic_output")
