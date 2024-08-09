from dagster import AssetDep, AssetSpec
from dagster_airlift.core import (
    AirflowInstance,
    BasicAuthBackend,
    PythonDefs,
    build_defs_from_airflow_instance,
    defs_from_factories,
)

from simple_migration.shared import t1_work, t2_work, t3_work

from .constants import A1, A2, A3, A4, AIRFLOW_BASE_URL, AIRFLOW_INSTANCE_NAME, PASSWORD, USERNAME

airflow_instance = AirflowInstance(
    auth_backend=BasicAuthBackend(
        webserver_url=AIRFLOW_BASE_URL, username=USERNAME, password=PASSWORD
    ),
    name=AIRFLOW_INSTANCE_NAME,
)

t1 = PythonDefs(
    name="simple__t1",
    specs=[AssetSpec(key=A1)],
    python_fn=t1_work,
)

t2 = PythonDefs(
    name="simple__t2",
    specs=[
        AssetSpec(key=A2, deps=[AssetDep(asset=A1)]),
        AssetSpec(key=A3, deps=[AssetDep(asset=A1)]),
    ],
    python_fn=t2_work,
)

t3 = PythonDefs(
    name="simple__t3",
    specs=[
        AssetSpec(key=A4, deps=[AssetDep(asset=A2), AssetDep(asset=A3)]),
    ],
    python_fn=t3_work,
)


defs = build_defs_from_airflow_instance(
    airflow_instance=airflow_instance, orchestrated_defs=defs_from_factories(t1, t2, t3)
)
