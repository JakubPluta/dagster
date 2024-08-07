import uuid

import responses
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.reconstruct import ReconstructableJob, ReconstructableRepository
from dagster._core.events import DagsterEventType
from dagster._core.execution.api import create_execution_plan, execute_plan
from dagster._core.instance_for_test import instance_for_test
from dagster._utils import file_relative_path
from dagster_powerbi import PowerBIWorkspace


def test_fetch_powerbi_workspace_data(workspace_data_api_mocks: None, workspace_id: str) -> None:
    fake_token = uuid.uuid4().hex
    resource = PowerBIWorkspace(
        api_token=fake_token,
        workspace_id=workspace_id,
    )

    actual_workspace_data = resource.fetch_powerbi_workspace_data()
    assert len(actual_workspace_data.dashboards_by_id) == 1
    assert len(actual_workspace_data.reports_by_id) == 1
    assert len(actual_workspace_data.semantic_models_by_id) == 1
    assert len(actual_workspace_data.data_sources_by_id) == 2


def test_translator_dashboard_spec(workspace_data_api_mocks: None, workspace_id: str) -> None:
    fake_token = uuid.uuid4().hex
    resource = PowerBIWorkspace(
        api_token=fake_token,
        workspace_id=workspace_id,
    )
    all_asset_specs = resource.build_asset_specs()

    # 1 dashboard, 1 report, 1 semantic model, 2 data sources
    assert len(all_asset_specs) == 5

    # Sanity check outputs, translator tests cover details here
    dashboard_spec = next(spec for spec in all_asset_specs if spec.key.path[0] == "dashboard")
    assert dashboard_spec.key.path == ["dashboard", "Sales_Returns_Sample_v201912"]

    report_spec = next(spec for spec in all_asset_specs if spec.key.path[0] == "report")
    assert report_spec.key.path == ["report", "Sales_Returns_Sample_v201912"]

    semantic_model_spec = next(
        spec for spec in all_asset_specs if spec.key.path[0] == "semantic_model"
    )
    assert semantic_model_spec.key.path == ["semantic_model", "Sales_Returns_Sample_v201912"]

    data_source_specs = [
        spec
        for spec in all_asset_specs
        if spec.key.path[0] not in ("dashboard", "report", "semantic_model")
    ]
    assert len(data_source_specs) == 2

    data_source_keys = {spec.key for spec in data_source_specs}
    assert data_source_keys == {
        AssetKey(["data_27_09_2019.xlsx"]),
        AssetKey(["sales_marketing_datas.xlsx"]),
    }


def test_using_cached_asset_data(workspace_data_api_mocks: responses.RequestsMock) -> None:
    with instance_for_test() as instance:
        assert len(workspace_data_api_mocks.calls) == 0

        from .pending_repo import pending_repo_from_cached_asset_metadata

        assert len(workspace_data_api_mocks.calls) == 5

        # first, we resolve the repository to generate our cached metadata
        repository_def = pending_repo_from_cached_asset_metadata.compute_repository_definition()

        # 5 PowerBI external assets, one materializable asset
        assert len(repository_def.assets_defs_by_key) == 5 + 1

        job_def = repository_def.get_job("all_asset_job")
        repository_load_data = repository_def.repository_load_data

        recon_repo = ReconstructableRepository.for_file(
            file_relative_path(__file__, "pending_repo.py"),
            fn_name="pending_repo_from_cached_asset_metadata",
        )
        recon_job = ReconstructableJob(repository=recon_repo, job_name="all_asset_job")

        execution_plan = create_execution_plan(recon_job, repository_load_data=repository_load_data)

        run = instance.create_run_for_job(job_def=job_def, execution_plan=execution_plan)

        events = execute_plan(
            execution_plan=execution_plan,
            job=recon_job,
            dagster_run=run,
            instance=instance,
        )

        assert (
            len([event for event in events if event.event_type == DagsterEventType.STEP_SUCCESS])
            == 1
        ), "Expected two successful steps"

        assert len(workspace_data_api_mocks.calls) == 5
