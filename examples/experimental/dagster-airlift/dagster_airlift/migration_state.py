from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional

import yaml


class TaskMigrationState(NamedTuple):
    migrated: bool

    @staticmethod
    def from_dict(task_dict: Dict[str, Any]) -> "TaskMigrationState":
        if "migrated" not in task_dict:
            raise Exception("Expected a 'migrated' key in the task dictionary")
        if set(task_dict.keys()) != {"migrated"}:
            raise Exception("Expected only a 'migrated' key in the task dictionary")
        if task_dict["migrated"] not in [True, False]:
            raise Exception("Expected 'migrated' key to be a boolean")
        return TaskMigrationState(migrated=task_dict["migrated"])


class DagMigrationState(NamedTuple):
    tasks: Dict[str, TaskMigrationState]

    @staticmethod
    def from_dict(dag_dict: Dict[str, Dict[str, Any]]) -> "DagMigrationState":
        if "tasks" not in dag_dict:
            raise Exception(
                f"Expected a 'tasks' key in the dag dictionary. Instead; got: {dag_dict}"
            )
        task_migration_states = {}
        for task_id, task_dict in dag_dict["tasks"].items():
            task_migration_states[task_id] = TaskMigrationState.from_dict(task_dict)
        return DagMigrationState(tasks=task_migration_states)

    def is_task_migrated(self, task_id: str) -> bool:
        if task_id not in self.tasks:
            return False
        return self.tasks[task_id].migrated


class AirflowMigrationState(NamedTuple):
    dags: Dict[str, DagMigrationState]

    def get_migration_state_for_task(self, dag_id: str, task_id: str) -> Optional[bool]:
        if dag_id not in self.dags:
            return None
        if task_id not in self.dags[dag_id].tasks:
            return None
        return self.dags[dag_id].tasks[task_id].migrated

    def dag_is_being_migrated(self, dag_id: str) -> bool:
        return self.get_migration_dict_for_dag(dag_id) is not None

    def get_migration_dict_for_dag(self, dag_id: str) -> Optional[Dict[str, Dict[str, Any]]]:
        if dag_id not in self.dags:
            return None
        return {
            "tasks": {
                task_id: {"migrated": task_state.migrated}
                for task_id, task_state in self.dags[dag_id].tasks.items()
            }
        }


class MigrationStateParsingError(Exception):
    pass


def load_migration_state_from_yaml(migration_yaml_path: Path) -> AirflowMigrationState:
    # Expect migration_yaml_path to be a directory, where each file represents a dag, and each
    # file in the subdir represents a task. The dictionary each task should consist of a single bit:
    # migrated: true/false.
    dag_migration_states = {}
    try:
        for dag_file in migration_yaml_path.iterdir():
            # Check that the file is a yaml file or yml file
            if dag_file.suffix not in [".yaml", ".yml"]:
                continue
            dag_id = dag_file.stem
            yaml_dict = yaml.safe_load(dag_file.read_text())
            if not isinstance(yaml_dict, dict):
                raise Exception("Expected a dictionary")
            dag_migration_states[dag_id] = DagMigrationState.from_dict(yaml_dict)
    except Exception as e:
        raise MigrationStateParsingError("Error parsing migration yaml") from e
    return AirflowMigrationState(dags=dag_migration_states)
