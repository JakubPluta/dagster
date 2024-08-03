import json
import logging
from typing import Any, Dict

from airflow import DAG
from airflow.models import BaseOperator

from dagster_airlift.in_airflow.dagster_operator import build_dagster_task

from ..migration_state import AirflowMigrationState


def mark_as_dagster_migrating(
    *,
    global_vars: Dict[str, Any],
    migration_state: AirflowMigrationState,
    logger: logging.Logger = logging.getLogger("dagster_airlift"),
) -> None:
    """Alters all airflow dags in the current context to be marked as migrating to dagster.
    Uses a migration dictionary to determine the status of the migration for each task within each dag.
    Should only ever be the last line in a dag file.

    Args:
        global_vars (Dict[str, Any]): The global variables in the current context. In most cases, retrieved with `globals()` (no import required).
        migration_state (AirflowMigrationState): The migration state for the dags.
        logger (Optional[logging.Logger]): The logger to use. Defaults to logging.getLogger("dagster_airlift").
    """
    caller_module = global_vars.get("__module__")
    suffix = f" in module `{caller_module}`" if caller_module else ""
    logger.debug(f"Searching for dags migrating to dagster{suffix}...")
    dag_vars_to_mark = set()
    task_vars_to_migrate = set()
    all_dags_by_id = {}
    for var, obj in global_vars.items():
        if isinstance(obj, DAG):
            dag: DAG = obj
            if migration_state.dag_is_being_migrated(obj.dag_id):
                logger.debug(f"Dag with id `{dag.dag_id}` has migration state.")
                dag_vars_to_mark.add(var)
            else:
                logger.debug(
                    f"Dag with id `{dag.dag_id} has no associated migration state. Skipping..."
                )
            all_dags_by_id[obj.dag_id] = obj
        if isinstance(obj, BaseOperator) and migration_state.get_migration_state_for_task(
            dag_id=obj.dag_id, task_id=obj.task_id
        ):
            task_vars_to_migrate.add(var)

    for var in dag_vars_to_mark:
        dag: DAG = global_vars[var]
        logger.debug(f"Tagging dag {dag.dag_id} as migrating.")
        dag.tags.append(
            json.dumps(
                {"DAGSTER_MIGRATION_STATUS": migration_state.get_migration_dict_for_dag(dag.dag_id)}
            )
        )
    logging.debug(f"Marked {len(dag_vars_to_mark)} dags as migrating to dagster via tag.")

    for var in task_vars_to_migrate:
        original_op: BaseOperator = global_vars[var]
        logger.debug(
            f"Creating new operator for task {original_op.task_id} in dag {original_op.dag_id}"
        )
        new_op = build_dagster_task(
            task_id=original_op.task_id,
            owner=original_op.owner,
            email=original_op.email,
            email_on_retry=original_op.email_on_retry,
            email_on_failure=original_op.email_on_failure,
            retries=original_op.retries,
            retry_delay=original_op.retry_delay,
            retry_exponential_backoff=original_op.retry_exponential_backoff,
            max_retry_delay=original_op.max_retry_delay,
            start_date=original_op.start_date,
            end_date=original_op.end_date,
            depends_on_past=original_op.depends_on_past,
            wait_for_downstream=original_op.wait_for_downstream,
            params=original_op.params,
            doc_md="This task has been migrated to dagster.",
        )
        original_op.dag.task_dict[original_op.task_id] = new_op

        new_op.upstream_task_ids = original_op.upstream_task_ids
        new_op.downstream_task_ids = original_op.downstream_task_ids
        new_op.dag = original_op.dag
        original_op.dag = None
        logger.debug(
            f"Switching global state var to dagster operator for task {original_op.task_id}."
        )
        global_vars[var] = new_op
    logging.debug(f"Marked {len(task_vars_to_migrate)} tasks as migrating to dagster.")
    logging.debug(f"Completed marking dags and tasks as migrating to dagster{suffix}.")
