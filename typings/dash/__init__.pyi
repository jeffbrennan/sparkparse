from typing import Any

class _CallbackContext:
    triggered: list[dict[str, Any]]
    triggered_prop_ids: dict[str, Any]
    triggered_id: Any
    inputs: dict[str, Any]
    states: dict[str, Any]
    args_grouping: Any
    outputs_grouping: Any
    response: Any
    inputs_list: list[Any]
    states_list: list[Any]
    outputs_list: list[Any]
    using_args_grouping: bool
    using_outputs_grouping: bool
    timing_information: dict[str, Any]
    cookies: dict[str, Any]
    headers: dict[str, Any]
    path: str
    remote: str
    origin: str

    def set_props(self, component_id: str | dict, props: dict) -> None: ...
    def record_timing(
        self, name: str, duration: float | None = ..., description: str | None = ...
    ) -> None: ...

callback_context: _CallbackContext

class Dash:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    server: Any
    layout: Any
    index_string: str
    page_container: Any

    def run(self, *args: Any, **kwargs: Any) -> None: ...

page_container: Any

def register_page(*args: Any, **kwargs: Any) -> None: ...

class _ComponentProp:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

class Input(_ComponentProp): ...
class Output(_ComponentProp): ...
class State(_ComponentProp): ...

def callback(*args: Any, **kwargs: Any) -> Any: ...

dcc: Any
html: Any
dash_table: Any

def get_app() -> Dash: ...
def set_props(component_id: str | dict, props: dict) -> None: ...
