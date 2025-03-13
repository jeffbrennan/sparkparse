import re
from enum import StrEnum, auto
from typing import Annotated, Any, ClassVar, Type

import polars as pl
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
    root_validator,
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
    BuildRight = auto()
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


class QueryEvent(BaseModel):
    query_id: int
    event_type: EventType
    query_time: int


def str_to_list(v: Any) -> list[str]:
    if not isinstance(v, str):
        raise TypeError(f"Expected a string, got {type(v)}")

    v_list = v.removeprefix("[").removesuffix("]").split(",")
    return [item.strip() for item in v_list if item.strip()]


class ScanDetail(BaseModel):
    output: Annotated[list[str], Field(alias="Output"), BeforeValidator(str_to_list)]
    batched: bool = Field(alias="Batched")
    location: str = Field(alias="Location")
    read_schema: str = Field(alias="ReadSchema")


class ColumnarToRowDetail(BaseModel):
    input: Annotated[list[str], Field("Input"), BeforeValidator(str_to_list)]


class ProjectDetail(BaseModel):
    input: Annotated[list[str], Field("Input"), BeforeValidator(str_to_list)]
    output: Annotated[list[str], Field("Output"), BeforeValidator(str_to_list)]


class HashAggregateDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    keys: Annotated[list[str], Field(alias="Keys"), BeforeValidator(str_to_list)]
    functions: Annotated[
        list[str], Field(alias="Functions"), BeforeValidator(str_to_list)
    ]
    aggregate_attributes: Annotated[
        list[str], Field(alias="Aggregate Attributes"), BeforeValidator(str_to_list)
    ]
    results: Annotated[list[str], Field(alias="Results"), BeforeValidator(str_to_list)]


class ExchangeType(StrEnum):
    ENSURE_REQUIREMENTS = "ENSURE_REQUIREMENTS"


class ExchangeArgument(BaseModel):
    partition_cols: list[str]
    n_partitions: int
    exchange_type: ExchangeType
    plan_identifier: int


class ExchangeDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: ExchangeArgument = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_exchange_argument_str(cls, value: Any) -> ExchangeArgument:
        hash_partition_section = (
            value.split("), ")[0].removeprefix("hashpartitioning(").split(",")
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
    output: Annotated[list[str], Field(alias="Output"), BeforeValidator(str_to_list)]
    stage_order: int = Field(alias="Arguments")


class AQEShuffleReadArgument(StrEnum):
    COALESCED = auto()


class AQEShuffleReadDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: AQEShuffleReadArgument = Field(alias="Arguments")


class SortArgumentCol(BaseModel):
    name: str
    asc: bool
    nulls_first: bool


class SortArgument(BaseModel):
    cols: list[SortArgumentCol]
    global_sort: bool
    sort_order: int


class SortDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: SortArgument = Field(alias="Arguments")

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_sort_argument_str(cls, value: Any) -> SortArgument:
        cols = []

        print("---")
        print(value)
        col_section = value.split("]")[0].removesuffix("[").strip().split(",")
        for i in col_section:
            col_raw = i.strip().split(" ")
            name = col_raw[0]
            asc = True if col_raw[1] == "ASC" else False
            nulls_first = True if col_raw[2] == "NULLS FIRST" else False
            cols.append(SortArgumentCol(name=name, asc=asc, nulls_first=nulls_first))

        global_sort = value.split("]")[1].strip() == "true"

        sort_order = value.split(",")[-1].strip()

        return SortArgument(
            cols=cols, global_sort=global_sort, sort_order=int(sort_order)
        )


class SortMergeJoinDetail(BaseModel):
    left_keys: Annotated[
        list[str], Field(alias="Left keys"), BeforeValidator(str_to_list)
    ]
    right_keys: Annotated[
        list[str], Field(alias="Right keys"), BeforeValidator(str_to_list)
    ]
    join_type: Annotated[
        list[str], Field(alias="Join type"), BeforeValidator(str_to_list)
    ]
    join_condition: Annotated[
        list[str], Field(alias="Join condition"), BeforeValidator(str_to_list)
    ]


class WindowDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: Annotated[
        list[str], Field(alias="Arguments"), BeforeValidator(str_to_list)
    ]


class WindowGroupLimitDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: Annotated[
        list[str], Field(alias="Arguments"), BeforeValidator(str_to_list)
    ]


class FilterDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    condition: Annotated[
        list[str], Field(alias="Condition"), BeforeValidator(str_to_list)
    ]


class CoalesceDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: Annotated[
        list[str], Field(alias="Arguments"), BeforeValidator(str_to_list)
    ]


class InsertIntoHadoopFsRelationCommandDetail(BaseModel):
    input: Annotated[list[str], Field(alias="Input"), BeforeValidator(str_to_list)]
    arguments: Annotated[
        list[str], Field(alias="Arguments"), BeforeValidator(str_to_list)
    ]


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
    )


class PhysicalPlanDetails(BaseModel):
    details: list[PhysicalPlanDetail]
    codegen_lookup: dict[int, int]


class PhysicalPlan(BaseModel):
    query_id: int
    details: PhysicalPlanDetails
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
}
