import json
from enum import StrEnum, auto
from typing import Annotated, Any, Type

import polars as pl
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
)


class EventType(StrEnum):
    start = auto()
    end = auto()


class Job(BaseModel):
    job_id: int = Field(alias="Job ID")
    event_type: EventType
    job_timestamp: int
    stages: list[int] | None = Field(alias="Stage IDs", default=None)


class Stage(BaseModel):
    stage_id: int
    event_type: EventType
    stage_timestamp: int


class Metric(BaseModel):
    name: str
    accumulator_id: int
    metric_type: str


class NodeType(StrEnum):
    @staticmethod
    def _generate_next_value_(name, *args) -> str:
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


class QueryEvent(BaseModel):
    query_id: int
    event_type: EventType
    query_time: int
    query_function: QueryFunction | None = None


def str_to_list(v: Any) -> list[str] | None:
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


class ScanDetail(BaseModel):
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


class ColumnarToRowDetail(BaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]


class ProjectDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]


class Function(BaseModel):
    function: str
    col: str | None


class HashAggregateDetail(BaseModel):
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
        if value.strip() == "[]":
            return None

        functions_split = value.removeprefix("[").removesuffix("]").split(", ")
        functions = []
        for f in functions_split:
            f_split = f.split("(")
            function = f_split[0]
            col = f_split[1].removesuffix(")")
            functions.append(Function(function=function, col=col))
        return functions


class ExchangeType(StrEnum):
    ENSURE_REQUIREMENTS = "ENSURE_REQUIREMENTS"


class ExchangeArgument(BaseModel):
    partition_cols: list[str] | None
    n_partitions: int
    exchange_type: ExchangeType
    plan_identifier: int


class ExchangeDetail(BaseModel):
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


class ShuffleQueryStageDetail(BaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    stage_order: int = Field(alias="Arguments")


class AQEShuffleReadArgument(StrEnum):
    COALESCED = auto()
    LOCAL = auto()


class AQEShuffleReadDetail(BaseModel):
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


class SortDetail(BaseModel):
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


class SortMergeJoinDetail(BaseModel):
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


class WindowDetail(BaseModel):
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


class WindowGroupLimitDetail(BaseModel):
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


class FilterDetail(BaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    condition: Annotated[list[FilterDetailCondition], Field(alias="Condition")]

    @field_validator("condition", mode="before")
    @classmethod
    def parse_filter_condition_str(cls, value: Any) -> list[FilterDetailCondition]:
        filter_split = value.split(") ")
        filter_conditions = []
        for i, filter_str in enumerate(filter_split):
            if filter_str == "":
                continue
            if i == 0:
                condition = Condition("AND")
            else:
                condition = Condition(filter_str.split(" ")[0].strip())

            if "isnotnull" in filter_str or "isnull" in filter_str:
                col = filter_str.split("(")[1].removesuffix(")")
                operator = "!=" if "isnot" in filter_str else "=="
                filter_conditions.append(
                    FilterDetailCondition(
                        condition=condition,
                        col=col,
                        operator=Operator(operator),
                        value=None,
                    )
                )
                continue

            col = filter_str.split(" ")[0].removeprefix("(")
            operator = Operator(filter_str.split(" ")[1])
            value = filter_str.split(" ")[2].removesuffix(")")
            filter_conditions.append(
                FilterDetailCondition(
                    condition=condition, col=col, operator=operator, value=value
                )
            )

        return filter_conditions


class CoalesceDetail(BaseModel):
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


class InsertIntoHadoopFsRelationCommandDetail(BaseModel):
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


class LocalTableScanDetail(BaseModel):
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


class WriteFilesDetail(BaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]


class LocalLimitDetail(BaseModel):
    input: Annotated[
        list[str] | None, Field(alias="Input"), BeforeValidator(str_to_list)
    ]
    limit: int = Field(alias="Arguments")


class GlobalLimitArguments(BaseModel):
    limit: int
    offset: int


class GlobalLimitDetail(BaseModel):
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


class BroadcastExchangeDetail(BaseModel):
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


class BroadcastQueryStageDetail(BaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    stage_order: int = Field(alias="Arguments")


class BroadcastHashJoinDetail(BaseModel):
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


class TakeOrderedAndProjectDetail(BaseModel):
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


class BroadcastNestedLoopJoinDetail(BaseModel):
    join_type: JoinType = Field(alias="Join type")
    join_condition: str | None = Field(alias="Join condition", default=None)

    @field_validator("join_condition", mode="before")
    def parse_join_condition_str(cls, value: Any) -> str | None:
        if value.strip() == "None":
            return None
        return value


class ReusedExchangeDetail(BaseModel):
    output: Annotated[
        list[str] | None, Field(alias="Output"), BeforeValidator(str_to_list)
    ]
    reuses_node_id: int = Field(alias="reuses_node_id")


class PhysicalPlanDetail(BaseModel):
    node_id: int
    node_type: NodeType
    detail: (
        ScanDetail
        | ColumnarToRowDetail
        | ProjectDetail
        | HashAggregateDetail
        | ExchangeDetail
        | ShuffleQueryStageDetail
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


class Task(BaseModel):
    task_id: int = Field(alias="Task ID")
    stage_id: int = Field(alias="Stage ID")
    index: int = Field(alias="Index")
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
    metrics: Metrics
    accumulators: list[Accumulator]


class DriverAccumUpdates(BaseModel):
    query_id: int
    accumulator_id: int
    update: int


class ParsedLog(BaseModel):
    name: str
    jobs: list[Job]
    stages: list[Stage]
    tasks: list[Task]
    queries: list[PhysicalPlan]
    query_times: list[QueryEvent]
    driver_accum_updates: list[DriverAccumUpdates]


class OutputFormat(StrEnum):
    csv = "csv"
    parquet = "parquet"
    delta = "delta"
    json = "json"


class ParsedLogDataFrames(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    combined: pl.DataFrame
    dag: pl.DataFrame


NODE_ID_PATTERN = r".*\((\d+)\)"
NODE_TYPE_PATTERN = r"(\b\w+\b).*\(\d{1,4}\)"
NODE_TYPE_DETAIL_MAP: dict[NodeType, Type[BaseModel]] = {
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
}
