from enum import StrEnum, auto

from pydantic import BaseModel, Field


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
    Sort = auto()
    WriteFiles = auto()
    Exchange = auto()
    Project = auto()
    BroadcastHashJoin = auto()
    ColumnarToRow = auto()
    Scan = auto()


class PhysicalPlanNode(BaseModel):
    node_id: int
    node_type: NodeType
    is_whole_stage_codegen: bool
    child_nodes: list[int] | None


class PhysicalPlan(BaseModel):
    sources: list[str]
    targets: list[str]
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
    corrupt_merged_block_chunks: int = Field(alias="Corrupt Merged Block Chunks")
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
    remote_blocks_fetched: int = Field(alias="Remote Blocks Fetched")
    local_blocks_fetched: int = Field(alias="Local Blocks Fetched")
    fetch_wait_time: int = Field(alias="Fetch Wait Time")
    remote_bytes_read: int = Field(alias="Remote Bytes Read")
    remote_bytes_read_to_disk: int = Field(alias="Remote Bytes Read To Disk")
    local_bytes_read: int = Field(alias="Local Bytes Read")
    total_records_read: int = Field(alias="Total Records Read")
    remote_requests_duration: int = Field(alias="Remote Requests Duration")
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


class ParsedLog(BaseModel):
    name: str
    jobs: list[Job]
    stages: list[Stage]
    tasks: list[Task]
    plan: PhysicalPlan


class OutputFormat(StrEnum):
    csv = "csv"
    parquet = "parquet"
    delta = "delta"
    json = "json"
