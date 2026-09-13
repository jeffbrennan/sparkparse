from pathlib import Path
from types import SimpleNamespace

from dash_ag_grid import AgGrid

from sparkparse.dataset import Dataset
from sparkparse.pages import home

FULL_LOGS = Path(__file__).parent / "data" / "full_logs"


def test_home_grid_constructs_with_pagination_options(monkeypatch):
    dataset = Dataset(FULL_LOGS)
    fake_app = SimpleNamespace(server=SimpleNamespace(config={"DATASET": dataset}))
    monkeypatch.setattr(home, "get_app", lambda: fake_app)

    grid, style = home.get_log_table(dataset.list_logs(), False)

    assert isinstance(grid, AgGrid)
    assert getattr(grid, "dashGridOptions") == {
        "pagination": True,
        "paginationPageSize": 25,
    }
    assert style == {}
