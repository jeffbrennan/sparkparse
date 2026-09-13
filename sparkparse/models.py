import base64
import datetime
import io
import json
from enum import StrEnum, auto
from typing import Annotated, Any

import polars as pl
from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)


class EventType(StrEnum):
    start = auto()
    end = auto()


class Job(BaseModel):
    job_id: int = Field(alias="Job ID")
    event_type: EventType
    job_timestamp: int
    stages: list[int] | None = Field(alias="Stage IDs", default=None)


class StageStatus(StrEnum):
    """Outcome of one stage attempt.

    ``running`` means the attempt was submitted but no completion event was
    observed — an incomplete log, not a stage that took zero time.
    """

    succeeded = "succeeded"
    failed = "failed"
    running = "running"


class Stage(BaseModel):
    """One stage *attempt*. ``stage_id`` alone is not a unique key: a retried
    stage emits a second Submitted/Completed pair with a higher attempt id."""

    stage_id: int
    stage_attempt_id: int = 0
    event_type: EventType
    stage_timestamp: int | None = None
    num_tasks: int | None = None
    status: StageStatus = StageStatus.running
    failure_reason: str | None = None


class Metric(BaseModel):
    name: str
    accumulator_id: int
    metric_type: str


class NodeType(StrEnum):
    @staticmethod
    def _generate_next_value_(
        name: str, start: int, count: int, last_values: list[Any]
    ) -> str:
        return name

    Sort = auto()
    WriteFiles = auto()
    Exchange = auto()
    Project = auto()
    BroadcastHashJoin = auto()
    ColumnarToRow = auto()
    Scan = auto()
    InsertIntoHadoopFsRelationCommand = auto()
    AQEShuffleRead = auto()
    ShuffleQueryStage = auto()
    HashAggregate = auto()
    BroadcastQueryStage = auto()
    BroadcastExchange = auto()
    Filter = auto()
    GlobalLimit = auto()
    LocalLimit = auto()
    TakeOrderedAndProject = auto()
    Union = auto()
    LocalTableScan = auto()
    Coalesce = auto()
    Window = auto()
    WindowGroupLimit = auto()
    SortMergeJoin = auto()
    TableCacheQueryStage = auto()
    InMemoryTableScan = auto()
    InMemoryRelation = auto()
    WholeStageCodegen = auto()
    BroadcastNestedLoopJoin = auto()
    ReusedExchange = auto()
    Generate = auto()
    AdaptiveSparkPlan = auto()
    ObjectHashAggregate = auto()
    SortAggregate = auto()
    Expand = auto()
    ArrowEvalPython = auto()
    BatchEvalPython = auto()
    MapInPandas = auto()
    MapInArrow = auto()
    PythonMapInArrow = auto()
    FlatMapGroupsInPandas = auto()
    FlatMapCoGroupsInPandas = auto()
    BatchScan = auto()
    WriteToDataSourceV2 = auto()
    AppendData = auto()
    OverwriteByExpression = auto()
    OverwritePartitionsDynamic = auto()
    SubqueryExec = auto()
    ReusedSubqueryExec = auto()
    SubqueryBroadcast = auto()
    RepartitionByExpression = auto()
    Sample = auto()
    Range = auto()
    CartesianProduct = auto()
    ArrowEvalPythonUDTF = auto()
    BatchEvalPythonUDTF = auto()
    FlatMapGroupsInArrow = auto()
    FlatMapCoGroupsInArrow = auto()
    TransformWithStateExec = auto()
    TransformWithStateInPandas = auto()
    TransformWithStateInPySpark = auto()
    SubqueryAdaptiveBroadcast = auto()
    ResultQueryStage = auto()
    CollectLimit = auto()
    Unknown = auto()


class Accumulator(BaseModel):
    task_id: int
    accumulator_id: int = Field(alias="ID")
    name: str = Field(alias="Name")
    update: int = Field(alias="Update")
    value: int = Field(alias="Value")
    internal: bool = Field(alias="Internal")
    count_failed_values: bool = Field(alias="Count Failed Values")
    metadata: str | None = Field(alias="Metadata", default=None)


class PlanAccumulator(BaseModel):
    node_id: int
    node_name: str
    node_string: str
    child_index: int  # debugging
    metric_name: str = Field(alias="name")
    accumulator_id: int = Field(alias="accumulatorId")
    metric_type: str = Field(alias="metricType")
    is_wholestage_codegen: bool = False


class PhysicalPlanNode(BaseModel):
    node_id: int
    node_type: NodeType
    child_nodes: list[int] | None = None
    whole_stage_codegen_id: int | None = None
    accumulators: list[PlanAccumulator] | None = None
    details: str | None = None  # based on PhysicalPlanDetail


class QueryFunction(StrEnum):
    COUNT = "count"
    SAVE = "save"
    CREATE_OR_REPLACE_TEMP_VIEW = "createOrReplaceTempView"
    SHOW = "show"
    COLLECT = "collect"
    FIRST = "first"
    HEAD = "head"
    TAKE = "take"
    FOREACH = "foreach"
    FOREACH_PARTITION = "foreachPartition"
    TO_LOCAL_ITERATOR = "toLocalIterator"


class QueryEvent(BaseModel):
    query_id: int
    event_type: EventType
    query_time: int
    query_function: QueryFunction | None = None


def str_to_list(v: Any) -> list[str] | None:
    if v is None:
        return None
    if isinstance(v, list):
        return v
    if not isinstance(v, str):
        raise TypeError(f"Expected a string, got {type(v)}")

    v = v.removeprefix("[").removesuffix("]").strip()
    if v == "":
        return None

    # project parsing
    col_definitions = []
    if " AS " in v:
        if v.count(" AS ") == v.count(", "):
            return v.split(", ")

        parts = v.split(" AS ")
        for i, part in enumerate(parts):
            if i == 0:
                col_definitions.append(part)
                continue
            split = part.split(", ")
            col_definitions[-1] = f"{col_definitions[-1]} AS {split[0]}"
            col_definitions.extend(split[1:])

        return col_definitions

    v_list = v.removeprefix("[").removesuffix("]").split(",")
    return [item.strip() for item in v_list if item.strip()]


class LocationType(StrEnum):
    IN_MEMORY_FILE_INDEX = "InMemoryFileIndex"


class ScanDetailLocation(BaseModel):
    location_type: LocationType
    location: list[str]


class DetailBaseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class ScanDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    batched: bool = Field(alias="Batched")
    location: ScanDetailLocation = Field(alias="Location")
    read_schema: str = Field(alias="ReadSchema")

    @field_validator("location", mode="before")
    @classmethod
    def parse_scan_detail_location_str(cls, value: Any) -> ScanDetailLocation:
        value_split = value.split(" [")
        location_type = LocationType(value_split[0])
        locations_raw = value_split[1].removesuffix("]").strip().split(",")

        locations = [i.removeprefix("file:") for i in locations_raw]
        return ScanDetailLocation(location_type=location_type, location=locations)


def deserialize_scan_detail(s: str) -> ScanDetail:
    data = json.loads(s)["detail"]
    data["location"] = ScanDetailLocation.model_construct(**data["location"])
    return ScanDetail.model_construct(**data)


class ColumnarToRowDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]


class ProjectDetail(DetailBaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]


class Function(BaseModel):
    function: str
    col: str | None


def _parse_function_str(f: str) -> Function:
    name = f.split("(")[0]
    args_str = f[len(name) + 1 : -1]
    args = [arg.strip() for arg in args_str.split(",") if arg.strip()]
    if not args:
        return Function(function=name, col=None)
    if len(args) == 1:
        return Function(function=name, col=args[0])
    return Function(function=name, col=args_str)


class HashAggregateDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    keys: Annotated[list[str] | None, Field(alias="Keys"), BeforeValidator(str_to_list)]
    functions: list[Function] | None = Field(alias="Functions")
    aggregate_attributes: Annotated[
        list[str] | None,
        Field(alias="Aggregate Attributes"),
        BeforeValidator(str_to_list),
    ]
    results: Annotated[
        list[str] | None, Field(alias="Results"), BeforeValidator(str_to_list)
    ]

    @field_validator("functions", mode="before")
    @classmethod
    def parse_hash_aggregate_function_str(cls, value: Any) -> list[Function] | None:
        if isinstance(value, list):
            return value
        if value.strip() == "[]":
            return None

        value = value.removeprefix("[").removesuffix("]").strip()
        functions: list[Function] = []
        current = ""
        depth = 0
        for char in value:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                if current.strip():
                    functions.append(_parse_function_str(current.strip()))
                current = ""
                continue
            current += char
        if current.strip():
            functions.append(_parse_function_str(current.strip()))
        return functions


class ExchangeType(StrEnum):
    ENSURE_REQUIREMENTS = "ENSURE_REQUIREMENTS"
    REPARTITION_BY_COL = "REPARTITION_BY_COL"
    REPARTITION_BY_NUM = "REPARTITION_BY_NUM"
    REPARTITION = "REPARTITION"
    REQUIRED_BY_STATEFUL_OPERATOR = "REQUIRED_BY_STATEFUL_OPERATOR"


class ExchangeArgument(BaseModel):
    partition_cols: list[str] | None
    n_partitions: int
    exchange_type: ExchangeType
    plan_identifier: int


class ExchangeDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: ExchangeArgument = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_exchange_argument_str(cls, value: Any) -> ExchangeArgument:
        if "SinglePartition" in value:
            return ExchangeArgument(
                partition_cols=None,
                n_partitions=1,
                exchange_type=ExchangeType.ENSURE_REQUIREMENTS,
                plan_identifier=int(value.split("plan_id=")[1].removesuffix("]")),
            )

        hash_partition_section = (
            value.split("), ")[0].removeprefix("hashpartitioning(").split(", ")
        )
        cols = [i for i in hash_partition_section if "#" in i]
        n_partitions = hash_partition_section[-1].strip()

        exchange_type_str = value.split("), ")[1].split(",")[0].strip()
        exchange_type = ExchangeType(exchange_type_str)
        plan_id = value.split("plan_id=")[1].removesuffix("]")
        return ExchangeArgument(
            partition_cols=cols,
            n_partitions=int(n_partitions),
            exchange_type=exchange_type,
            plan_identifier=int(plan_id),
        )


class ShuffleQueryStageDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    stage_order: int = Field(alias="Arguments")


class ResultQueryStageDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    stage_order: int = Field(alias="Arguments")


class AQEShuffleReadArgument(StrEnum):
    COALESCED = auto()
    LOCAL = auto()


class AQEShuffleReadDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: AQEShuffleReadArgument = Field(alias="Arguments")


class SortArgumentCol(BaseModel):
    name: str
    asc: bool
    nulls_first: bool


class SortArgument(BaseModel):
    cols: list[SortArgumentCol]
    global_sort: bool
    sort_order: int


def parse_sort_argument_col_str(col_section: str) -> list[SortArgumentCol]:
    col_split = col_section.split(", ")
    sort_args = []
    for i in col_split:
        col_raw = i.strip().split(" ")
        name = col_raw[0].removeprefix("[")
        asc = True if col_raw[1] == "ASC" else False
        nulls_first = True if col_raw[3] == "FIRST" else False
        sort_args.append(SortArgumentCol(name=name, asc=asc, nulls_first=nulls_first))
    return sort_args


class SortDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: SortArgument = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_sort_argument_str(cls, value: Any) -> SortArgument:
        col_section = value.split("]")[0].removesuffix("[").strip()
        cols = parse_sort_argument_col_str(col_section)

        global_sort = value.split("]")[1].strip() == "true"
        sort_order = value.split(",")[-1].strip()

        return SortArgument(
            cols=cols, global_sort=global_sort, sort_order=int(sort_order)
        )


class JoinType(StrEnum):
    LEFT_OUTER = "LeftOuter"
    LEFT_SEMI = "LeftSemi"
    LEFT_ANTI = "LeftAnti"
    RIGHT_OUTER = "RightOuter"
    FULL_OUTER = "FullOuter"
    INNER = "Inner"
    CROSS = "Cross"


class SortMergeJoinDetail(DetailBaseModel):
    left_keys: Annotated[
        list[str] | None, Field(alias="Left keys"), BeforeValidator(str_to_list)
    ]
    right_keys: Annotated[
        list[str] | None, Field(alias="Right keys"), BeforeValidator(str_to_list)
    ]
    join_type: JoinType = Field(alias="Join type")
    join_condition: str | None = Field(alias="Join condition", default=None)

    @field_validator("join_condition", mode="before")
    def parse_join_condition_str(cls, value: Any) -> str | None:
        if value.strip() == "None":
            return None
        return value


class WindowSpecification(BaseModel):
    partition_cols: list[str]
    order_cols: list[SortArgumentCol]
    window_frame: str


class WindowDetailArgument(BaseModel):
    window_function: Function
    window_specification: WindowSpecification


class WindowDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: WindowDetailArgument = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_window_detail_argument_str(cls, value: Any) -> WindowDetailArgument:
        function_section = value.split(") ")[0].removeprefix("[").strip() + ")"

        window_function_col = function_section.split("(")[1].removesuffix(")")
        window_function_col = None if window_function_col == "" else window_function_col
        window_function = Function(
            function=function_section.split("(")[0], col=window_function_col
        )

        window_specification = value.split("windowspecdefinition(")[1].split("], ")[0]
        pre_frame_section = window_specification.split(", specifiedwindowframe")[0]
        partition_cols = [
            i for i in pre_frame_section.split(", ") if not ("DESC" in i or "ASC" in i)
        ]
        order_cols = [
            i for i in pre_frame_section.split(", ") if "DESC" in i or "ASC" in i
        ]

        order_cols_parsed = []
        for col in order_cols:
            order_cols_parsed.append(
                SortArgumentCol(
                    name=col.split(" ")[0],
                    asc=True if col.split(" ")[1] == "ASC" else False,
                    nulls_first=True if col.split(" ")[2] == "NULLS FIRST" else False,
                )
            )

        window_frame = window_specification.split("specifiedwindowframe(")[1].split(
            "], "
        )[0]

        window_frame = "specifiedwindowframe(" + window_frame.replace(")))", "))")

        return WindowDetailArgument(
            window_function=window_function,
            window_specification=WindowSpecification(
                partition_cols=partition_cols,
                order_cols=order_cols_parsed,
                window_frame=window_frame,
            ),
        )


class ProcessingStage(StrEnum):
    FINAL = "Final"
    PARTIAL = "Partial"


class WindowGroupLimitArgument(BaseModel):
    partition_cols: list[str]
    order_cols: list[SortArgumentCol]
    window_function: Function
    limit: int
    processing_stage: ProcessingStage


class WindowGroupLimitDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: WindowGroupLimitArgument = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_window_group_limit_argument_str(
        cls, value: Any
    ) -> WindowGroupLimitArgument:
        partition_cols = value.split("], ")[0].removeprefix("[").strip().split(", ")
        order_section = value.split("], ")[1].removeprefix("[").strip().split(", ")

        order_cols = []
        for col in order_section:
            col_split = col.split(" ")
            order_cols.append(
                SortArgumentCol(
                    name=col_split[0],
                    asc=True if col_split[1] == "ASC" else False,
                    nulls_first=True if col_split[2] == "NULLS FIRST" else False,
                )
            )

        function_section = ", ".join(
            value.split("], ")[2].removeprefix("[").strip().split(", ")[0:-2]
        )

        window_function = Function(
            function=function_section.split("(")[0],
            col=function_section.split("(")[1].removesuffix(")"),
        )

        limit = int(value.split(", ")[-2].strip())

        processing_stage = ProcessingStage(value.split(", ")[-1])

        return WindowGroupLimitArgument(
            partition_cols=partition_cols,
            order_cols=order_cols,
            window_function=window_function,
            limit=int(limit),
            processing_stage=ProcessingStage(processing_stage),
        )


class Operator(StrEnum):
    ge = ">="
    le = "<="
    eq = "=="
    ne = "!="
    gt = ">"
    lt = "<"
    in_ = "in"
    not_in = "not in"
    like = "like"


class Condition(StrEnum):
    and_ = "AND"
    or_ = "OR"


class FilterDetailCondition(BaseModel):
    condition: Condition
    col: str
    operator: Operator
    value: str | int | None


class FilterDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    condition: str = Field(alias="Condition")


class CoalesceDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    n_partitions: int = Field(alias="Arguments")


class Option(BaseModel):
    key: str
    value: str


class WriteMode(StrEnum):
    OVERWRITE = "Overwrite"
    APPEND = "Append"
    ERROR_IF_EXISTS = "ErrorIfExists"
    IGNORE = "Ignore"


class InsertIntoHadoopFsRelationCommandDetailArguments(BaseModel):
    file_path: str
    error_if_exists: bool
    format: str
    options: list[Option]
    mode: WriteMode
    output: list[str] | None


class InsertIntoHadoopFsRelationCommandDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: InsertIntoHadoopFsRelationCommandDetailArguments = Field(
        alias="Arguments"
    )

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_insert_into_hadoop_fs_relation_command_detail_arguments_str(
        cls, value: Any
    ) -> InsertIntoHadoopFsRelationCommandDetailArguments:
        value_split = value.split(", ")
        file_path = value_split[0].removeprefix("file:")
        error_if_exists = value_split[1].strip() == "true"
        output_format = value_split[2].strip()

        options_parsed = []
        options_raw = value.split(", [")[1].split("]")[0].split(", ")
        for option in options_raw:
            options_parsed.append(
                Option(
                    key=option.split("=")[0],
                    value=option.split("=")[1].removesuffix("]").strip(),
                )
            )

        mode = WriteMode(value.split("], ")[1].split(", [")[0])

        output_raw = "[" + value.split(", [")[-1]
        output = str_to_list(output_raw)

        return InsertIntoHadoopFsRelationCommandDetailArguments(
            file_path=file_path,
            error_if_exists=error_if_exists,
            format=output_format,
            options=options_parsed,
            mode=mode,
            output=output,
        )


def deserialize_insert_into_hadoop_fs_relation_command_detail(
    s: str,
) -> InsertIntoHadoopFsRelationCommandDetail:
    data = json.loads(s)["detail"]
    data["arguments"] = (
        InsertIntoHadoopFsRelationCommandDetailArguments.model_construct(
            **data["arguments"]
        )
    )
    return InsertIntoHadoopFsRelationCommandDetail.model_construct(**data)


class LocalTableScanArguments(BaseModel):
    contents: str
    input: list[str] | None


class LocalTableScanDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    arguments: LocalTableScanArguments = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_local_table_scan_arguments_str(
        cls, value: Any
    ) -> LocalTableScanArguments:
        contents = value.split(", [")[0]
        input_col_section = "[" + value.split(", [")[1]
        input_cols = str_to_list(input_col_section)
        return LocalTableScanArguments(contents=contents, input=input_cols)


class WriteFilesDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]


class LocalLimitDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    limit: int = Field(alias="Arguments")


class GlobalLimitArguments(BaseModel):
    limit: int
    offset: int


class GlobalLimitDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: GlobalLimitArguments = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_global_limit_arguments_str(cls, value: Any) -> GlobalLimitArguments:
        value_split = value.split(", ")
        limit = int(value_split[0])
        offset = int(value_split[1])
        return GlobalLimitArguments(limit=limit, offset=offset)


class BroadcastExchangeMode(StrEnum):
    HASHED_RELATION_BROADCAST_MODE = "HashedRelationBroadcastMode"
    IDENTITY_BROADCAST_MODE = "IdentityBroadcastMode"


class BroadcastExchangeArguments(BaseModel):
    mode: BroadcastExchangeMode
    join_cols: str | None
    nullable: bool | None
    plan_identifier: int


class BroadcastExchangeDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: BroadcastExchangeArguments = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_broadcast_exchange_arguments_str(
        cls, value: Any
    ) -> BroadcastExchangeArguments:
        value_split = value.split(", ")
        mode = BroadcastExchangeMode(value_split[0].split("(")[0])
        plan_identifier = int(
            value.split(", [")[1].split("=")[1].removeprefix("#").removesuffix("]")
        )

        if mode == BroadcastExchangeMode.IDENTITY_BROADCAST_MODE:
            return BroadcastExchangeArguments(
                mode=mode,
                join_cols=None,
                nullable=None,
                plan_identifier=plan_identifier,
            )

        nullable = value.split("), [plan")[0].split(", ")[-1].strip() == "true"

        cols = (
            value.split("Mode(")[1]
            .split("), [")[0]
            .removesuffix("),false)")
            .removesuffix("),true)")
        )

        return BroadcastExchangeArguments(
            mode=mode,
            join_cols=cols,
            nullable=nullable,
            plan_identifier=plan_identifier,
        )


class BroadcastQueryStageDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    stage_order: int = Field(alias="Arguments")


class BroadcastHashJoinDetail(DetailBaseModel):
    left_keys: Annotated[
        list[str], Field(alias="Left keys"), BeforeValidator(str_to_list)
    ]
    right_keys: Annotated[
        list[str], Field(alias="Right keys"), BeforeValidator(str_to_list)
    ]
    join_type: JoinType = Field(alias="Join type")
    join_condition: str | None = Field(alias="Join condition", default=None)

    @field_validator("join_condition", mode="before")
    def parse_join_condition_str(cls, value: Any) -> str | None:
        if value.strip() == "None":
            return None
        return value


class TakeOrderedAndProjectDetailArguments(BaseModel):
    limit: int
    cols: list[SortArgumentCol]
    output: list[str] | None


class TakeOrderedAndProjectDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: TakeOrderedAndProjectDetailArguments = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_take_ordered_and_project_arguments_str(
        cls, value: Any
    ) -> TakeOrderedAndProjectDetailArguments:
        limit = int(value.split(", [")[0].strip())
        cols_raw = value.split(", [")[1].split("]")[0]
        cols = parse_sort_argument_col_str(cols_raw)
        output = str_to_list(value.split("], [")[-1])

        return TakeOrderedAndProjectDetailArguments(
            limit=limit, cols=cols, output=output
        )


class BroadcastNestedLoopJoinDetail(DetailBaseModel):
    join_type: JoinType = Field(alias="Join type")
    join_condition: str | None = Field(alias="Join condition", default=None)

    @field_validator("join_condition", mode="before")
    def parse_join_condition_str(cls, value: Any) -> str | None:
        if value.strip() == "None":
            return None
        return value


class ReusedExchangeDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    reuses_node_id: int = Field(alias="reuses_node_id")


class GenerateDetailArguments(BaseModel):
    generator: str
    include_nulls: bool
    output: list[str] | None


class GenerateDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    arguments: GenerateDetailArguments = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_generate_detail_arguments_str(cls, value: Any) -> GenerateDetailArguments:
        generator = value.split("), ")[0]
        include_nulls = ", true, " in value
        output = str_to_list(value.split(", [")[-1])

        return GenerateDetailArguments(
            generator=generator, include_nulls=include_nulls, output=output
        )


class TableCacheQueryStageDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    stage_order: int = Field(alias="Arguments")


class InMemoryTableScanDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]


class InMemoryRelationDetail(DetailBaseModel):
    # contains an entire nested plan - won't be parsed
    arguments: str = Field(alias="Arguments")


class AdaptiveSparkPlanDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    is_final_plan: bool = Field(alias="Arguments")

    @field_validator("is_final_plan", mode="before")
    @classmethod
    def parse_is_final_plan_str(cls, value: Any) -> bool:
        return value.split("=")[-1] == "true"


class RawDetail(DetailBaseModel):
    raw: str


class ObjectHashAggregateDetail(HashAggregateDetail):
    pass


class SortAggregateDetail(HashAggregateDetail):
    pass


class ExpandDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    projections: list[list[str]] = Field(alias="Arguments")
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ] = None

    @model_validator(mode="before")
    @classmethod
    def split_combined_arguments(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        args = data.get("Arguments")
        if args is None or "Output" in data:
            return data

        args = str(args).strip()
        # Spark sometimes merges projections and output into Arguments:
        #   [[...], [...]], [output_col1, output_col2]
        # Split at the last "]]", [" boundary when an output list follows.
        boundary = args.rfind("]], [")
        if boundary != -1:
            data["Arguments"] = args[: boundary + 2]
            data["Output"] = args[boundary + 4 :]
        return data

    @field_validator("projections", mode="before")
    @classmethod
    def parse_projections_str(cls, value: Any) -> list[list[str]]:
        if isinstance(value, list):
            return value

        value = value.strip()
        if value.startswith("["):
            value = value[1:]
        if value.endswith("]"):
            value = value[:-1]
        value = value.strip()
        if not value:
            return []

        if value.startswith("List("):
            raw_projections = value.split("), ")
            projections = []
            for i, proj in enumerate(raw_projections):
                proj = proj.strip()
                if not proj:
                    continue
                if i < len(raw_projections) - 1:
                    proj = proj + ")"
                proj = proj.removeprefix("List(").removesuffix(")")
                projections.append(
                    [item.strip() for item in proj.split(",") if item.strip()]
                )
            return projections

        raw_projections = value.split("], [")
        return [
            [
                item.strip()
                for item in proj.removeprefix("[").removesuffix("]").split(",")
                if item.strip()
            ]
            for proj in raw_projections
        ]


class ArrowEvalPythonDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    udfs: Annotated[list[str], Field(alias="Arguments"), BeforeValidator(str_to_list)]


class BatchEvalPythonDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    udfs: Annotated[list[str], Field(alias="Arguments"), BeforeValidator(str_to_list)]


class MapInPandasDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    func: str = Field(alias="Arguments")


class MapInArrowDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    func: str = Field(alias="Arguments")


class FlatMapGroupsInPandasDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    func: str = Field(alias="Arguments")
    grouping_keys: Annotated[
        list[str] | None, Field(alias="GroupingKeys"), BeforeValidator(str_to_list)
    ] = None


class FlatMapCoGroupsInPandasDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ] = None
    left_output: Annotated[
        list[str] | None, Field(alias="Left output"), BeforeValidator(str_to_list)
    ] = None
    right_output: Annotated[
        list[str] | None, Field(alias="Right output"), BeforeValidator(str_to_list)
    ] = None
    func: str = Field(alias="Arguments")


class ArrowEvalPythonUDTFDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    udtf: str = Field(alias="Arguments")


class BatchEvalPythonUDTFDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    udtf: str = Field(alias="Arguments")


class FlatMapGroupsInArrowDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    func: str = Field(alias="Arguments")
    grouping_keys: Annotated[
        list[str] | None, Field(alias="GroupingKeys"), BeforeValidator(str_to_list)
    ] = None


class FlatMapCoGroupsInArrowDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ] = None
    left_output: Annotated[
        list[str] | None, Field(alias="Left output"), BeforeValidator(str_to_list)
    ] = None
    right_output: Annotated[
        list[str] | None, Field(alias="Right output"), BeforeValidator(str_to_list)
    ] = None
    func: str = Field(alias="Arguments")


class TransformWithStateExecDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ] = None
    arguments: str | None = Field(alias="Arguments", default=None)


class TransformWithStateInPandasDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ] = None
    arguments: str | None = Field(alias="Arguments", default=None)


class TransformWithStateInPySparkDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ] = None
    arguments: str | None = Field(alias="Arguments", default=None)


class SubqueryAdaptiveBroadcastDetail(DetailBaseModel):
    name: str | None = Field(alias="Name", default=None)
    index: int | None = Field(alias="Index", default=None)
    indices: Annotated[
        list[str] | None, Field(alias="Indices"), BeforeValidator(str_to_list)
    ] = None
    build_keys: Annotated[
        list[str] | None, Field(alias="BuildKeys"), BeforeValidator(str_to_list)
    ] = None


class BatchScanDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    table: str | None = Field(alias="Table", default=None)
    filters: Annotated[
        list[str] | None, Field(alias="Filters"), BeforeValidator(str_to_list)
    ] = None
    runtime_filters: Annotated[
        list[str] | None, Field(alias="RuntimeFilters"), BeforeValidator(str_to_list)
    ] = None


class WriteToDataSourceV2Detail(DetailBaseModel):
    table: str | None = Field(alias="Table", default=None)
    write_options: dict[str, str] = Field(alias="WriteOptions", default_factory=dict)

    @field_validator("write_options", mode="before")
    @classmethod
    def parse_write_options_str(cls, value: Any) -> dict[str, str]:
        if isinstance(value, dict):
            return value
        value = str(value).strip()
        if value.startswith("["):
            value = value[1:]
        if value.endswith("]"):
            value = value[:-1]
        options: dict[str, str] = {}
        for item in value.split(","):
            item = item.strip()
            if "=" in item:
                key, val = item.split("=", 1)
                options[key.strip()] = val.strip()
        return options


class AppendDataDetail(DetailBaseModel):
    table: str | None = Field(alias="Table", default=None)
    query: str | None = Field(alias="Query", default=None)


class OverwriteByExpressionDetail(DetailBaseModel):
    table: str | None = Field(alias="Table", default=None)
    delete_condition: str | None = Field(alias="DeleteCondition", default=None)


class OverwritePartitionsDynamicDetail(DetailBaseModel):
    table: str | None = Field(alias="Table", default=None)


class SubqueryExecDetail(DetailBaseModel):
    name: str | None = Field(alias="Name", default=None)
    child_plan_id: int | None = Field(alias="ChildPlanId", default=None)


class ReusedSubqueryExecDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    reuses_node_id: int = Field(alias="reuses_node_id")


class SubqueryBroadcastDetail(DetailBaseModel):
    name: str | None = Field(alias="Name", default=None)
    index: int | None = Field(alias="Index", default=None)
    build_keys: Annotated[
        list[str] | None, Field(alias="BuildKeys"), BeforeValidator(str_to_list)
    ] = None


class RepartitionByExpressionDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    partition_exprs: Annotated[
        list[str] | None,
        Field(alias="PartitionExpressions"),
        BeforeValidator(str_to_list),
    ] = None
    n_partitions: int | None = Field(alias="NumPartitions", default=None)


class SampleDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    lower_bound: float | None = Field(alias="LowerBound", default=None)
    upper_bound: float | None = Field(alias="UpperBound", default=None)
    with_replacement: bool | None = Field(alias="WithReplacement", default=None)
    seed: int | None = Field(alias="Seed", default=None)

    @field_validator(
        "lower_bound", "upper_bound", "with_replacement", "seed", mode="before"
    )
    @classmethod
    def parse_sample_argument_str(cls, value: Any) -> float | bool | int | None:
        if value is None:
            return None
        value_str = str(value).strip()
        if value_str == "":
            return None
        if value_str.lower() == "true":
            return True
        if value_str.lower() == "false":
            return False
        if "." in value_str:
            return float(value_str)
        return int(value_str)


class RangeDetail(DetailBaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    start: int | None = Field(alias="Start", default=None)
    end: int | None = Field(alias="End", default=None)
    step: int | None = Field(alias="Step", default=None)
    n_partitions: int | None = Field(alias="NumPartitions", default=None)

    @field_validator("start", "end", "step", "n_partitions", mode="before")
    @classmethod
    def parse_range_argument_str(cls, value: Any) -> int | None:
        if value is None:
            return None
        return int(str(value).strip())


class CartesianProductDetail(DetailBaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ] = None
    join_type: str | None = Field(
        validation_alias=AliasChoices("Join type", "JoinType"), default=None
    )
    join_condition: str | None = Field(
        validation_alias=AliasChoices("Join condition", "JoinCondition"), default=None
    )


class PhysicalPlanDetail(DetailBaseModel):
    node_id: int
    node_type: NodeType
    detail: (
        ScanDetail
        | ColumnarToRowDetail
        | ProjectDetail
        | HashAggregateDetail
        | ObjectHashAggregateDetail
        | SortAggregateDetail
        | ExpandDetail
        | ExchangeDetail
        | ShuffleQueryStageDetail
        | ResultQueryStageDetail
        | AQEShuffleReadDetail
        | SortDetail
        | SortMergeJoinDetail
        | WindowDetail
        | WindowGroupLimitDetail
        | FilterDetail
        | CoalesceDetail
        | InsertIntoHadoopFsRelationCommandDetail
        | LocalTableScanDetail
        | WriteFilesDetail
        | LocalLimitDetail
        | GlobalLimitDetail
        | BroadcastExchangeDetail
        | BroadcastQueryStageDetail
        | BroadcastHashJoinDetail
        | TakeOrderedAndProjectDetail
        | BroadcastNestedLoopJoinDetail
        | ReusedExchangeDetail
        | GenerateDetail
        | TableCacheQueryStageDetail
        | InMemoryTableScanDetail
        | InMemoryRelationDetail
        | AdaptiveSparkPlanDetail
        | RawDetail
        | ArrowEvalPythonDetail
        | BatchEvalPythonDetail
        | MapInPandasDetail
        | MapInArrowDetail
        | FlatMapGroupsInPandasDetail
        | FlatMapCoGroupsInPandasDetail
        | ArrowEvalPythonUDTFDetail
        | BatchEvalPythonUDTFDetail
        | FlatMapGroupsInArrowDetail
        | FlatMapCoGroupsInArrowDetail
        | TransformWithStateExecDetail
        | TransformWithStateInPandasDetail
        | TransformWithStateInPySparkDetail
        | SubqueryAdaptiveBroadcastDetail
        | BatchScanDetail
        | WriteToDataSourceV2Detail
        | AppendDataDetail
        | OverwriteByExpressionDetail
        | OverwritePartitionsDynamicDetail
        | SubqueryExecDetail
        | ReusedSubqueryExecDetail
        | SubqueryBroadcastDetail
        | RepartitionByExpressionDetail
        | SampleDetail
        | RangeDetail
        | CartesianProductDetail
        | None
    )


class PhysicalPlanDetails(BaseModel):
    details: list[PhysicalPlanDetail]
    codegen_lookup: dict[int, int]


class PhysicalPlan(BaseModel):
    query_id: int
    query_function: QueryFunction | None = None
    nodes: list[PhysicalPlanNode]


class ExecutorMetrics(BaseModel):
    jvm_heap_memory: int = Field(alias="JVMHeapMemory")
    jvm_offheap_memory: int = Field(alias="JVMOffHeapMemory")
    onheap_execution_memory: int = Field(alias="OnHeapExecutionMemory")
    offheap_execution_memory: int = Field(alias="OffHeapExecutionMemory")
    onheap_storage_memory: int = Field(alias="OnHeapStorageMemory")
    offheap_storage_memory: int = Field(alias="OffHeapStorageMemory")
    onheap_unified_memory: int = Field(alias="OnHeapUnifiedMemory")
    offheap_unified_memory: int = Field(alias="OffHeapUnifiedMemory")
    direct_pool_memory: int = Field(alias="DirectPoolMemory")
    mapped_pool_memory: int = Field(alias="MappedPoolMemory")
    process_tree_jvm_vmemory: int = Field(alias="ProcessTreeJVMVMemory")
    process_tree_jvm_rss_memory: int = Field(alias="ProcessTreeJVMRSSMemory")
    process_tree_python_vmemory: int = Field(alias="ProcessTreePythonVMemory")
    process_tree_python_rss_memory: int = Field(alias="ProcessTreePythonRSSMemory")
    process_tree_other_vmemory: int = Field(alias="ProcessTreeOtherVMemory")
    process_tree_other_rss_memory: int = Field(alias="ProcessTreeOtherRSSMemory")
    minor_gc_count: int = Field(alias="MinorGCCount")
    minor_gc_time: int = Field(alias="MinorGCTime")
    major_gc_count: int = Field(alias="MajorGCCount")
    major_gc_time: int = Field(alias="MajorGCTime")
    total_gc_time: int = Field(alias="TotalGCTime")


class PushBasedShuffle(BaseModel):
    merged_corrupt_block_chunks: int = Field(alias="Corrupt Merged Block Chunks")
    merged_fetch_fallback_count: int = Field(alias="Merged Fetch Fallback Count")
    merged_remote_blocks_fetched: int = Field(alias="Merged Remote Blocks Fetched")
    merged_local_blocks_fetched: int = Field(alias="Merged Local Blocks Fetched")
    merged_remote_chunks_fetched: int = Field(alias="Merged Remote Chunks Fetched")
    merged_local_chunks_fetched: int = Field(alias="Merged Local Chunks Fetched")
    merged_remote_bytes_read: int = Field(alias="Merged Remote Bytes Read")
    merged_local_bytes_read: int = Field(alias="Merged Local Bytes Read")
    merged_remote_requests_duration: int = Field(
        alias="Merged Remote Requests Duration"
    )


class ShuffleReadMetrics(BaseModel):
    shuffle_remote_blocks_fetched: int = Field(alias="Remote Blocks Fetched")
    shuffle_local_blocks_fetched: int = Field(alias="Local Blocks Fetched")
    shuffle_fetch_wait_time: int = Field(alias="Fetch Wait Time")
    shuffle_remote_bytes_read: int = Field(alias="Remote Bytes Read")
    shuffle_remote_bytes_read_to_disk: int = Field(alias="Remote Bytes Read To Disk")
    shuffle_local_bytes_read: int = Field(alias="Local Bytes Read")
    shuffle_records_read: int = Field(alias="Total Records Read")
    shuffle_remote_requests_duration: int = Field(alias="Remote Requests Duration")
    push_based_shuffle: PushBasedShuffle = Field(alias="Push Based Shuffle")


class ShuffleWriteMetrics(BaseModel):
    shuffle_bytes_written: int = Field(alias="Shuffle Bytes Written")
    shuffle_write_time: int = Field(alias="Shuffle Write Time")
    shuffle_records_written: int = Field(alias="Shuffle Records Written")


class InputMetrics(BaseModel):
    bytes_read: int = Field(alias="Bytes Read")
    records_read: int = Field(alias="Records Read")


class OutputMetrics(BaseModel):
    bytes_written: int = Field(alias="Bytes Written")
    records_written: int = Field(alias="Records Written")


class TaskMetrics(BaseModel):
    executor_deserialize_time: int = Field(alias="Executor Deserialize Time")
    executor_deserialize_cpu_time: int = Field(alias="Executor Deserialize CPU Time")
    executor_run_time: int = Field(alias="Executor Run Time")
    executor_cpu_time: int = Field(alias="Executor CPU Time")
    peak_execution_memory: int = Field(alias="Peak Execution Memory")
    result_size: int = Field(alias="Result Size")
    jvm_gc_time: int = Field(alias="JVM GC Time")
    result_serialization_time: int = Field(alias="Result Serialization Time")
    memory_bytes_spilled: int = Field(alias="Memory Bytes Spilled")
    disk_bytes_spilled: int = Field(alias="Disk Bytes Spilled")


class Metrics(BaseModel):
    task_metrics: TaskMetrics
    executor_metrics: ExecutorMetrics
    shuffle_read_metrics: ShuffleReadMetrics
    shuffle_write_metrics: ShuffleWriteMetrics
    input_metrics: InputMetrics
    output_metrics: OutputMetrics


class TaskStatus(StrEnum):
    """Outcome of one task attempt, taken from ``Task End Reason``."""

    success = "success"
    failed = "failed"
    killed = "killed"


class Task(BaseModel):
    task_id: int = Field(alias="Task ID")
    stage_id: int = Field(alias="Stage ID")
    stage_attempt_id: int = Field(alias="Stage Attempt ID", default=0)
    index: int = Field(alias="Index")
    partition_id: int | None = Field(alias="Partition ID", default=None)
    attempt: int = Field(alias="Attempt")
    task_start_time: int = Field(alias="Launch Time")
    task_finish_time: int = Field(alias="Finish Time")
    executor_id: str = Field(alias="Executor ID")
    host: str = Field(alias="Host")
    task_type: str = Field(alias="Task Type")
    task_loc: str = Field(alias="Locality")
    speculative: bool = Field(alias="Speculative")
    failed: bool = Field(alias="Failed")
    killed: bool = Field(alias="Killed")
    status: TaskStatus = TaskStatus.success
    failure_reason: str | None = None
    # A task that died before its metrics were serialized has no metrics at
    # all. None says "not measured"; zeros would claim the task did no work.
    metrics: Metrics | None = None
    accumulators: list[Accumulator] = Field(default_factory=list)


class DriverAccumUpdates(BaseModel):
    query_id: int
    accumulator_id: int
    update: int


class EventLogCodec(StrEnum):
    """Compression codecs recognized on event-log file names.

    Only ``none``, ``zstd`` and ``gz`` can actually be read: Spark's ``lz4``,
    ``lzf`` and ``snappy`` event logs use Java-specific block framing
    (``LZ4BlockOutputStream``, Xerial Snappy) that no Python codec reads. Those
    are recognized so the failure names the codec instead of surfacing as a
    JSON decode error.
    """

    none = "none"
    zstd = "zstd"
    gz = "gz"
    lz4 = "lz4"
    lzf = "lzf"
    snappy = "snappy"


READABLE_CODECS: frozenset[EventLogCodec] = frozenset(
    {EventLogCodec.none, EventLogCodec.zstd, EventLogCodec.gz}
)


class EventLogSegment(BaseModel):
    """One physical file belonging to a logical event log."""

    uri: str
    index: int | None = None
    codec: EventLogCodec = EventLogCodec.none
    in_progress: bool = False


class EventLogSource(BaseModel):
    """A logical event log: one application, one or more ordered segments."""

    name: str
    application_id: str | None = None
    attempt_id: str | None = None
    segments: list[EventLogSegment] = Field(default_factory=list)
    rolling: bool = False
    complete: bool = True
    modified: float | None = None
    root_uri: str | None = None

    @property
    def uris(self) -> list[str]:
        return [segment.uri for segment in self.segments]

    @property
    def codecs(self) -> list[EventLogCodec]:
        return sorted({segment.codec for segment in self.segments})


class LogDiagnostic(BaseModel):
    """One recoverable problem encountered while reading an event log."""

    code: str
    message: str
    uri: str | None = None
    line: int | None = None


class ParsedLog(BaseModel):
    name: str
    jobs: list[Job]
    stages: list[Stage]
    tasks: list[Task]
    queries: list[PhysicalPlan]
    query_times: list[QueryEvent]
    driver_accum_updates: list[DriverAccumUpdates]
    source: EventLogSource | None = None
    diagnostics: list[LogDiagnostic] = Field(default_factory=list)
    spark_version: str | None = None
    application_id: str | None = None
    application_name: str | None = None
    unknown_events: dict[str, int] = Field(default_factory=dict)
    events_read: int = 0


class OutputFormat(StrEnum):
    csv = "csv"
    parquet = "parquet"
    delta = "delta"
    json = "json"


class ParsedLogDataFrames(BaseModel):
    """Parsed output frames.

    ``combined`` holds one row per task *attempt*. ``job_stage`` and
    ``query_stage`` are association tables: a stage can belong to several jobs
    and serve several queries, and joining those relations into ``combined``
    would duplicate task rows and double-count their metrics.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    combined: pl.DataFrame
    dag: pl.DataFrame
    job_stage: pl.DataFrame | None = None
    query_stage: pl.DataFrame | None = None
    diagnostics: list[LogDiagnostic] = Field(default_factory=list)


class CaptureStatus(StrEnum):
    complete = "complete"
    partial = "partial"
    failed = "failed"


class CapabilityStatus(StrEnum):
    not_applicable = "not_applicable"
    available = "available"
    partial = "partial"
    unavailable = "unavailable"
    unknown = "unknown"


class CaptureCapability(BaseModel):
    """Coverage for one family of metrics in a capture.

    ``query_coverage`` uses string query IDs so the contract remains JSON
    serializable even when a backend uses non-integer execution identifiers.
    """

    status: CapabilityStatus = CapabilityStatus.unknown
    reason: str | None = None
    source: str | None = None
    query_coverage: dict[str, CapabilityStatus] = Field(default_factory=dict)


class CaptureCapabilities(BaseModel):
    """Explicit, source-neutral metric coverage for a capture."""

    plan_structure: CaptureCapability = Field(default_factory=CaptureCapability)
    operator_metrics: CaptureCapability = Field(default_factory=CaptureCapability)
    query_elapsed_time: CaptureCapability = Field(default_factory=CaptureCapability)
    task_metrics: CaptureCapability = Field(default_factory=CaptureCapability)
    stage_timing: CaptureCapability = Field(default_factory=CaptureCapability)
    scan_details: CaptureCapability = Field(default_factory=CaptureCapability)
    join_details: CaptureCapability = Field(default_factory=CaptureCapability)


class CaptureMetadata(BaseModel):
    """Identity and lifecycle metadata attached to every capture result."""

    capture_id: str
    backend: str
    transport: str | None = None
    compute_type: str | None = None
    access_mode: str | None = None
    source_application_id: str | None = None
    source_session_id: str | None = None
    client_version: str | None = None
    runtime_version: str | None = None
    capture_start: datetime.datetime
    capture_end: datetime.datetime | None = None
    status: CaptureStatus = CaptureStatus.complete
    schema_version: str = "1"
    workload_label: str | None = None
    configuration: dict[str, Any] = Field(default_factory=dict)


class CaptureDiagnostic(BaseModel):
    """A recoverable or terminal issue observed while producing a result."""

    code: str
    message: str
    phase: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class CaptureResult(BaseModel):
    """Versioned capture artifact containing data, coverage, and diagnostics."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dag: pl.DataFrame
    combined: pl.DataFrame
    metadata: CaptureMetadata
    capabilities: CaptureCapabilities
    diagnostics: list[CaptureDiagnostic] = Field(default_factory=list)

    @field_validator("dag", "combined", mode="before")
    @classmethod
    def coerce_frame(cls, value: Any) -> pl.DataFrame:
        if isinstance(value, pl.DataFrame):
            return value
        if isinstance(value, dict) and value.get("format") == "arrow-ipc":
            return pl.read_ipc(
                io.BytesIO(base64.b64decode(value["data"], validate=True))
            )
        raise TypeError(f"Expected a Polars DataFrame, got {type(value)!r}")

    @field_serializer("dag", "combined")
    def serialize_frame(self, value: pl.DataFrame) -> dict[str, str]:
        buffer = io.BytesIO()
        value.write_ipc(buffer)
        return {
            "format": "arrow-ipc",
            "data": base64.b64encode(buffer.getvalue()).decode("ascii"),
        }


NODE_ID_PATTERN = r".*\((\d+)\)"
NODE_TYPE_PATTERN = r"(\b\w+\b).*\(\d{1,4}\)"
NODE_TYPE_DETAIL_MAP: dict[NodeType, type[BaseModel]] = {
    NodeType.Scan: ScanDetail,
    NodeType.ColumnarToRow: ColumnarToRowDetail,
    NodeType.Project: ProjectDetail,
    NodeType.HashAggregate: HashAggregateDetail,
    NodeType.Exchange: ExchangeDetail,
    NodeType.ShuffleQueryStage: ShuffleQueryStageDetail,
    NodeType.AQEShuffleRead: AQEShuffleReadDetail,
    NodeType.Sort: SortDetail,
    NodeType.SortMergeJoin: SortMergeJoinDetail,
    NodeType.Window: WindowDetail,
    NodeType.WindowGroupLimit: WindowGroupLimitDetail,
    NodeType.Filter: FilterDetail,
    NodeType.Coalesce: CoalesceDetail,
    NodeType.InsertIntoHadoopFsRelationCommand: InsertIntoHadoopFsRelationCommandDetail,
    NodeType.LocalTableScan: LocalTableScanDetail,
    NodeType.WriteFiles: WriteFilesDetail,
    NodeType.LocalLimit: LocalLimitDetail,
    NodeType.GlobalLimit: GlobalLimitDetail,
    NodeType.BroadcastExchange: BroadcastExchangeDetail,
    NodeType.BroadcastQueryStage: BroadcastQueryStageDetail,
    NodeType.BroadcastHashJoin: BroadcastHashJoinDetail,
    NodeType.TakeOrderedAndProject: TakeOrderedAndProjectDetail,
    NodeType.BroadcastNestedLoopJoin: BroadcastNestedLoopJoinDetail,
    NodeType.ReusedExchange: ReusedExchangeDetail,
    NodeType.Generate: GenerateDetail,
    NodeType.TableCacheQueryStage: TableCacheQueryStageDetail,
    NodeType.InMemoryTableScan: InMemoryTableScanDetail,
    NodeType.InMemoryRelation: InMemoryRelationDetail,
    NodeType.AdaptiveSparkPlan: AdaptiveSparkPlanDetail,
    NodeType.ObjectHashAggregate: ObjectHashAggregateDetail,
    NodeType.SortAggregate: SortAggregateDetail,
    NodeType.Expand: ExpandDetail,
    NodeType.ArrowEvalPython: ArrowEvalPythonDetail,
    NodeType.BatchEvalPython: BatchEvalPythonDetail,
    NodeType.MapInPandas: MapInPandasDetail,
    NodeType.MapInArrow: MapInArrowDetail,
    NodeType.PythonMapInArrow: MapInArrowDetail,
    NodeType.FlatMapGroupsInPandas: FlatMapGroupsInPandasDetail,
    NodeType.FlatMapCoGroupsInPandas: FlatMapCoGroupsInPandasDetail,
    NodeType.BatchScan: BatchScanDetail,
    NodeType.WriteToDataSourceV2: WriteToDataSourceV2Detail,
    NodeType.AppendData: AppendDataDetail,
    NodeType.OverwriteByExpression: OverwriteByExpressionDetail,
    NodeType.OverwritePartitionsDynamic: OverwritePartitionsDynamicDetail,
    NodeType.SubqueryExec: SubqueryExecDetail,
    NodeType.ReusedSubqueryExec: ReusedSubqueryExecDetail,
    NodeType.SubqueryBroadcast: SubqueryBroadcastDetail,
    NodeType.RepartitionByExpression: RepartitionByExpressionDetail,
    NodeType.Sample: SampleDetail,
    NodeType.Range: RangeDetail,
    NodeType.CartesianProduct: CartesianProductDetail,
    NodeType.ArrowEvalPythonUDTF: ArrowEvalPythonUDTFDetail,
    NodeType.BatchEvalPythonUDTF: BatchEvalPythonUDTFDetail,
    NodeType.FlatMapGroupsInArrow: FlatMapGroupsInArrowDetail,
    NodeType.FlatMapCoGroupsInArrow: FlatMapCoGroupsInArrowDetail,
    NodeType.TransformWithStateExec: TransformWithStateExecDetail,
    NodeType.TransformWithStateInPandas: TransformWithStateInPandasDetail,
    NodeType.TransformWithStateInPySpark: TransformWithStateInPySparkDetail,
    NodeType.SubqueryAdaptiveBroadcast: SubqueryAdaptiveBroadcastDetail,
    NodeType.ResultQueryStage: ResultQueryStageDetail,
}


RUN_RECORD_VERSION = 2


class RunRecord(BaseModel):
    """Compact, versioned snapshot of one run for trending and regression checks.

    Absent measures are ``None``; a measured zero stays ``0``. Timing keeps the
    wall-clock (``duration_s``) and aggregate (``cumulative_time_s``) meanings
    apart, and memory and disk spill stay separate rather than being summed.
    ``coverage`` records which capability families were available so a later
    comparison never reads a missing measure as a knowable zero.
    """

    record_version: int = RUN_RECORD_VERSION
    run_id: str
    run_at: datetime.datetime
    log_name: str
    status: str | None = None
    backend: str | None = None
    transport: str | None = None
    compute_type: str | None = None
    access_mode: str | None = None
    runtime_version: str | None = None
    client_version: str | None = None
    workload_fingerprint: str | None = None
    coverage: dict[str, str] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    # Wall-clock elapsed time across queries. Connect client-observed elapsed
    # time is only used when timestamps are unavailable.
    duration_s: float | None = None
    # Aggregate per-query time (sum of measured query durations). Distinct from
    # wall-clock elapsed and never a substitute for it.
    cumulative_time_s: float | None = None
    bytes_read: int | None = None
    bytes_written: int | None = None
    records_read: int | None = None
    records_written: int | None = None
    shuffle_read_bytes: int | None = None
    shuffle_write_bytes: int | None = None
    memory_bytes_spilled: int | None = None
    disk_bytes_spilled: int | None = None
    n_queries: int | None = None
    n_stages: int | None = None
    n_tasks: int | None = None
    n_cartesian_joins: int | None = None
    max_node_duration_min: float | None = None
    max_scan_bytes: int | None = None


class MetricComparison(BaseModel):
    """One measure compared between the current run and its baseline cohort."""

    metric: str
    current: float | None
    baseline: float | None
    delta: float | None
    pct_change: float | None
    sample_count: int
    cohort: str | None = None


class ComparisonReport(BaseModel):
    """Result of comparing the latest run against comparable history."""

    log_name: str
    current_run_id: str
    current_run_at: datetime.datetime
    cohort_size: int
    window: int
    metrics: list[MetricComparison] = Field(default_factory=list)
    excluded: list[str] = Field(default_factory=list)
    plan_changed: bool | None = None


class MetricUnit(StrEnum):
    """Physical unit a normalized metric value is expressed in."""

    bytes = "bytes"
    milliseconds = "milliseconds"
    nanoseconds = "nanoseconds"
    rows = "rows"
    # Named ``items`` because ``count`` would shadow ``str.count`` on a StrEnum.
    items = "count"
    ratio = "ratio"
    seconds = "seconds"
    none = "none"


class MetricScope(StrEnum):
    """Entity a metric describes."""

    query = "query"
    operator = "operator"
    stage = "stage"
    task = "task"
    unknown = "unknown"


class MetricAggregation(StrEnum):
    """How a metric combines across tasks or across a plan subtree."""

    sum = "sum"
    max = "max"
    last = "last"
    median = "median"
    cumulative = "cumulative"
    unknown = "unknown"


class MetricDerivation(StrEnum):
    """Provenance of a normalized metric value."""

    measured = "measured"
    inferred_from_metric_type = "inferred_from_metric_type"
    derived = "derived"


class MetricDefinition(BaseModel):
    """One canonical metric and the raw names that map onto it."""

    canonical: str
    unit: MetricUnit
    scope: MetricScope
    aggregation: MetricAggregation
    aliases: frozenset[str]
    description: str

    model_config = ConfigDict(frozen=True)


class NormalizedMetric(BaseModel):
    """A raw plan/task metric expressed in canonical, machine-readable form.

    ``canonical`` is ``None`` for metrics with no verified mapping. Such metrics
    are preserved verbatim rather than guessed at.
    """

    raw_name: str
    canonical: str | None
    value: float | int | None
    unit: MetricUnit
    scope: MetricScope
    aggregation: MetricAggregation
    source: str
    derivation: MetricDerivation
    coverage: CapabilityStatus
    readable: str | None = None


class RuleStatus(StrEnum):
    """Whether an analysis rule could be evaluated against a capture."""

    evaluated = "evaluated"
    unsupported = "unsupported"
    insufficient_data = "insufficient_data"


class FindingSeverity(StrEnum):
    critical = "critical"
    warning = "warning"


class FindingConfidence(StrEnum):
    high = "high"
    medium = "medium"
    low = "low"


class EvidenceValue(BaseModel):
    """A single numeric or textual fact backing a finding."""

    name: str
    value: float | int | str | None
    unit: MetricUnit = MetricUnit.none
    source: str | None = None


class Finding(BaseModel):
    """One diagnostic observation with the evidence that produced it."""

    rule_id: str
    severity: FindingSeverity
    category: str
    observation: str
    confidence: FindingConfidence
    query_id: int | None = None
    stage_id: int | None = None
    node_ids: list[int] = Field(default_factory=list)
    evidence: list[EvidenceValue] = Field(default_factory=list)
    threshold: EvidenceValue | None = None
    caveat: str | None = None
    next_investigation: str | None = None


class RuleAssessment(BaseModel):
    """Outcome of running one rule, including why it could not run."""

    rule_id: str
    status: RuleStatus
    reason: str | None = None
    entities_evaluated: int = 0
    findings: int = 0


class AnalysisReport(BaseModel):
    """Findings plus per-rule assessment status for one capture."""

    schema_version: str = "1"
    log_name: str
    findings: list[Finding] = Field(default_factory=list)
    assessments: list[RuleAssessment] = Field(default_factory=list)


DATABRICKS_REPORT_VERSION = "1"
EXPERIMENT_SCHEMA_VERSION = "1"


class CollectionStatus(StrEnum):
    """Whether collection finished within its configured bounds."""

    complete = "complete"
    partial = "partial"
    failed = "failed"


class MetricCompleteness(StrEnum):
    """Coverage of an aggregate over the discovered query population."""

    complete = "complete"
    observed_subtotal = "observed_subtotal"
    unavailable = "unavailable"


class WorkflowCollectionLimits(BaseModel):
    """Bounds persisted with every report so partial collection is explainable."""

    deadline_seconds: float
    max_pages: int
    max_requests: int
    max_retries: int
    retry_backoff_seconds: float
    outputs: str = "failed"


class WorkflowRunIdentity(BaseModel):
    run_id: str
    job_id: str | None = None
    run_name: str | None = None
    run_type: str | None = None
    number_in_job: int | None = None
    original_attempt_run_id: str | None = None
    run_page_url: str | None = None
    workspace_host: str | None = None
    creator_user_name: str | None = None


class WorkflowRunStatus(BaseModel):
    life_cycle_state: str | None = None
    result_state: str | None = None
    state_message: str | None = None
    termination_code: str | None = None
    termination_type: str | None = None
    user_cancelled_or_timedout: bool | None = None


class WorkflowTiming(BaseModel):
    """Run-level timing.

    ``workflow_elapsed_ms`` is wall-clock and is never the sum of task durations:
    workflow tasks may overlap. ``summed_task_execution_ms`` is an aggregate, not
    an elapsed measure.
    """

    workflow_elapsed_ms: int | None = None
    setup_ms: int | None = None
    execution_ms: int | None = None
    cleanup_ms: int | None = None
    summed_task_execution_ms: int | None = None
    tasks_may_overlap: bool = True


class ComputeReference(BaseModel):
    """A task's compute snapshot.

    ``source`` is ``run_snapshot`` for fields embedded in the run response and
    ``current_lookup`` for a later cluster query, which describes the cluster
    now rather than at run time.
    """

    task_key: str
    environment_key: str | None = None
    cluster_id: str | None = None
    source: str = "run_snapshot"
    runtime_engine: str | None = None
    spark_version: str | None = None
    node_type_id: str | None = None
    num_workers: int | None = None
    data_security_mode: str | None = None
    performance_target: str | None = None


class RevisionEvidence(BaseModel):
    """Executed-code provenance, kept separate from asserted revisions.

    ``confidence`` is ``executed`` when a Git snapshot reports the used commit,
    ``asserted`` when only ``--revision`` was supplied, ``mixed`` when tasks
    disagree, and ``unknown`` otherwise. Branch names and local HEAD are never
    treated as proof of executed code.
    """

    executed_commit: str | None = None
    repository_url: str | None = None
    repository_provider: str | None = None
    branch: str | None = None
    commit_timestamp_ms: int | None = None
    scope: str | None = None
    asserted_revision: str | None = None
    artifact_digest: str | None = None
    confidence: str = "unknown"


class TaskTiming(BaseModel):
    setup_ms: int | None = None
    execution_ms: int | None = None
    cleanup_ms: int | None = None
    start_time_ms: int | None = None
    end_time_ms: int | None = None


class WorkflowTask(BaseModel):
    """One task run. Identity is retained even when its task type is unknown."""

    task_key: str
    run_id: str | None = None
    attempt_number: int | None = None
    original_attempt_run_id: str | None = None
    run_if: str | None = None
    state: str | None = None
    result_state: str | None = None
    timing: TaskTiming = Field(default_factory=TaskTiming)
    compute: ComputeReference | None = None
    task_kind: str | None = None
    is_nested_job: bool = False
    output_path: str | None = None
    output_status: str | None = None
    error_excerpt: str | None = None


class OutputAttachment(BaseModel):
    task_key: str
    run_id: str | None = None
    status: str
    path: str | None = None
    error: str | None = None
    excerpt: str | None = None


class QueryObservation(BaseModel):
    """One query's facts, kept once per query ID across recollections."""

    query_id: str
    status: str | None = None
    is_final: bool | None = None
    execution_end_time_ms: int | None = None
    collected_at: datetime.datetime
    job_id: str | None = None
    job_run_id: str | None = None
    job_task_run_id: str | None = None
    attribution: str = "unattributed"
    task_key: str | None = None
    warehouse_id: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class MetricAggregate(BaseModel):
    """One canonical measure aggregated over a matched query population.

    A subtotal over only the queries that reported a metric is an *observed
    subtotal*, never a complete total. ``observed_query_count`` is the matched
    query population; ``counted_query_count`` is how many reported the metric.
    """

    metric: str
    unit: MetricUnit
    source: str
    source_version: str
    aggregation: str
    observed_query_count: int
    counted_query_count: int
    value: float | None
    value_exact: int | None = None
    completeness: MetricCompleteness
    detail: str | None = None


class QueryMetrics(BaseModel):
    discovered_query_count: int
    matched_query_count: int
    attribution_counts: dict[str, int] = Field(default_factory=dict)
    aggregates: list[MetricAggregate] = Field(default_factory=list)
    discovery_complete: bool = False
    denied: bool = False
    observations: list[QueryObservation] = Field(default_factory=list)
    unavailable_measures: list[str] = Field(default_factory=list)


class ReportDiagnostic(BaseModel):
    code: str
    message: str
    severity: str = "warning"
    scope: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ReportObservation(BaseModel):
    """A measured fact plus an optional proposed cause, kept distinct."""

    kind: str
    summary: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    hypothesis: str | None = None


class RunReport(BaseModel):
    """Versioned, portable workflow-run report envelope."""

    schema_version: str = DATABRICKS_REPORT_VERSION
    collection_status: CollectionStatus = CollectionStatus.complete
    collected_at: datetime.datetime
    collection_time_ms: int | None = None
    identity: WorkflowRunIdentity
    status: WorkflowRunStatus
    timing: WorkflowTiming
    tasks: list[WorkflowTask] = Field(default_factory=list)
    query_metrics: QueryMetrics | None = None
    compute: list[ComputeReference] = Field(default_factory=list)
    revision: RevisionEvidence
    coverage: dict[str, str] = Field(default_factory=dict)
    limits: WorkflowCollectionLimits
    diagnostics: list[ReportDiagnostic] = Field(default_factory=list)
    observations: list[ReportObservation] = Field(default_factory=list)
    next_checks: list[str] = Field(default_factory=list)
    outputs: list[OutputAttachment] = Field(default_factory=list)


class TrialMetadata(BaseModel):
    """The experiment-specific metadata attached to one collected snapshot."""

    variant: str
    revision: str | None = None
    artifact_digest: str | None = None
    config_fingerprint: str | None = None
    input_snapshot: str | None = None
    warmup: bool = False
    correctness: str | None = None
    hypothesis: str | None = None
    notes: str | None = None
    workspace_host: str | None = None
    job_id: str | None = None


class TrialSnapshot(BaseModel):
    """A report plus its experiment context, stored immutably on disk."""

    schema_version: str = EXPERIMENT_SCHEMA_VERSION
    snapshot_id: str
    collected_at: datetime.datetime
    run_id: str
    workspace_host: str | None = None
    job_id: str | None = None
    trial: TrialMetadata
    report: RunReport


class TrialRef(BaseModel):
    trial_id: str
    variant: str
    run_id: str
    snapshot_id: str
    revision: str | None = None
    config_fingerprint: str | None = None
    input_snapshot: str | None = None
    workspace_host: str | None = None
    job_id: str | None = None
    created_at: datetime.datetime
    warmup: bool = False
    correctness: str | None = None


class ExperimentManifest(BaseModel):
    schema_version: str = EXPERIMENT_SCHEMA_VERSION
    experiment_id: str
    name: str | None = None
    objective: str | None = None
    workspace_host: str | None = None
    job_id: str | None = None
    baseline_trial_id: str | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    trials: list[TrialRef] = Field(default_factory=list)


class MetricDelta(BaseModel):
    metric: str
    unit: MetricUnit
    aggregation: str
    baseline_values: list[float]
    candidate_values: list[float]
    baseline_median: float | None = None
    candidate_median: float | None = None
    baseline_min: float | None = None
    baseline_max: float | None = None
    candidate_min: float | None = None
    candidate_max: float | None = None
    delta: float | None = None
    pct_change: float | None = None
    single_observation: bool = False
    caveat: str | None = None


class TaskDelta(BaseModel):
    task_key: str
    change: str
    baseline: dict[str, float | None] = Field(default_factory=dict)
    candidate: dict[str, float | None] = Field(default_factory=dict)
    deltas: dict[str, float | None] = Field(default_factory=dict)


class ComparisonGroup(BaseModel):
    label: str
    variant: str | None = None
    trial_ids: list[str] = Field(default_factory=list)
    run_ids: list[str] = Field(default_factory=list)
    snapshot_ids: list[str] = Field(default_factory=list)
    sample_count: int = 0
    excluded_failed: int = 0
    excluded_active: int = 0
    excluded_warmup: int = 0
    excluded_incomplete: int = 0


class ComparabilityNote(BaseModel):
    aspect: str
    status: str
    detail: str | None = None


class ExperimentComparison(BaseModel):
    schema_version: str = EXPERIMENT_SCHEMA_VERSION
    experiment_id: str
    baseline: ComparisonGroup
    candidate: ComparisonGroup
    metrics: list[MetricDelta] = Field(default_factory=list)
    tasks: list[TaskDelta] = Field(default_factory=list)
    comparability: list[ComparabilityNote] = Field(default_factory=list)
    revision_confidence: str = "unknown"
    notes: list[str] = Field(default_factory=list)
