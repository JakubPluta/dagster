from typing import Any, Dict, List, Literal, Optional

import boto3
import botocore
import dagster._check as check
from dagster import PipesClient
from dagster._annotations import experimental
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.utils import PipesEnvContextInjector, open_pipes_session

from dagster_aws.pipes.message_readers import PipesCloudWatchMessageReader


@experimental
class PipesECSClient(PipesClient, TreatAsResourceParam):
    """A pipes client for running AWS ECS tasks.

    Args:
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject
            context into the ECS task
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages
            from the ECS task. Defaults to :py:class:`PipesCloudWatchsMessageReader`.
        client (Optional[boto3.client]): The boto ECS client used to launch the ECS task
        forward_termination (bool): Whether to cancel the ECS task when the Dagster process receives a termination signal.
    """

    def __init__(
        self,
        client: Optional[boto3.client] = None,
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
        forward_termination: bool = True,
    ):
        self._client = client or boto3.client("ecs")
        self._context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader = message_reader or PipesCloudWatchMessageReader()
        self.forward_termination = check.bool_param(forward_termination, "forward_termination")

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    def run(
        self,
        *,
        context: OpExecutionContext,
        task_definition: str,
        extras: Optional[Dict[str, Any]] = None,
        count: int = 1,
        launch_type: Optional[Literal["EC2", "FARGATE", "EXTERNAL"]] = None,
        cluster: Optional[str] = None,
        group: Optional[str] = None,
        network_configuration: Optional[Dict[str, Any]] = None,
        overrides: Optional[Dict[str, Any]] = None,
        placement_constraints: Optional[List[Dict[str, Any]]] = None,
        placement_strategy: Optional[List[Dict[str, Any]]] = None,
        platform_version: Optional[str] = None,
        reference_id: Optional[str] = None,
        started_by: Optional[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        client_token: Optional[str] = None,
        volume_configurations: Optional[List[Dict[str, Any]]] = None,
        capacity_provider_strategy: Optional[List[Dict[str, Any]]] = None,
        enable_ecs_managed_tags: Optional[bool] = None,
        enable_execute_command: Optional[bool] = None,
        propagate_tags: Optional[Literal["TASK_DEFINITION", "SERVICE", "NONE"]] = None,
    ) -> PipesClientCompletedInvocation:
        """Start a ECS task, enriched with the pipes protocol.

        See also: `AWS API Documentation <https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_RunTask.html>`_

        Returns:
            PipesClientCompletedInvocation: Wrapper containing results reported by the external
            process.
        """
        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
            extras=extras,
        ) as session:
            pipes_args = session.get_bootstrap_env_vars()

            overrides = overrides or {}
            overrides["containerOverrides"] = overrides.get("containerOverrides", [])

            # get all containers from task definition
            task_definition_response = self._client.describe_task_definition(
                taskDefinition=task_definition
            )

            all_container_names = {
                container["name"]
                for container in task_definition_response["taskDefinition"]["containerDefinitions"]
            }

            container_names_with_overrides = {
                container_override["name"]
                for container_override in overrides["containerOverrides"]
            }

            print(all_container_names)

            if isinstance(self._context_injector, PipesEnvContextInjector):
                # set env variables for every container in the taskDefinition
                # respecting current overrides provided by the user

                environment_overrides = [
                    {
                        "name": k,
                        "value": v,
                    }
                    for k, v in pipes_args.items()
                ]

                # set environment variables for existing overrides

                for container_override in overrides["containerOverrides"]:
                    container_override["environment"] = container_override.get("environment", [])
                    container_override["environment"].extend(environment_overrides)

                # set environment variables for containers that are not in the overrides
                for container_name in all_container_names - container_names_with_overrides:
                    overrides["containerOverrides"].append(
                        {
                            "name": container_name,
                            "environment": environment_overrides,
                        }
                    )

            print(overrides)

            params: Dict[str, Any] = {
                "taskDefinition": task_definition,
                "count": count,
                "launchType": launch_type,
                "cluster": cluster,
                "group": group,
                "networkConfiguration": network_configuration,
                "overrides": overrides,
                "placementConstraints": placement_constraints,
                "placementStrategy": placement_strategy,
                "platformVersion": platform_version,
                "referenceId": reference_id,
                "startedBy": started_by,
                "tags": tags,
                "clientToken": client_token,
                "volumeConfigurations": volume_configurations,
                "capacityProviderStrategy": capacity_provider_strategy,
                "enableECSManagedTags": enable_ecs_managed_tags,
                "enableExecuteCommand": enable_execute_command,
                "propagateTags": propagate_tags,
            }

            params = {
                k: v for k, v in params.items() if v is not None
            }  # boto3 doesn't use None as default values

            response = self._client.run_task(**params)

            tasks: List[str] = [task["taskArn"] for task in response["tasks"]]

            try:
                self._wait_for_tasks_completion(context=context, tasks=tasks, cluster=cluster)
            except DagsterExecutionInterruptedError:
                if self.forward_termination:
                    context.log.warning("Dagster process interrupted, terminating ECS tasks")
                    self._terminate_tasks(context=context, tasks=tasks, cluster=cluster)
                raise

            context.log.info(f"Task {tasks} completed")

            # if isinstance(self._message_reader, PipesCloudWatchMessageReader):

            #     self._message_reader.consume_cloudwatch_logs(
            #         f"{log_group}/output", run_id, start_time=int(start_timestamp)
            #     )

        return PipesClientCompletedInvocation(session)

    def _wait_for_tasks_completion(
        self, context: OpExecutionContext, tasks: List[str], cluster: Optional[str] = None
    ):
        waiter = self._client.get_waiter("tasks_stopped")

        params: Dict[str, Any] = {"tasks": tasks}

        if cluster:
            params["cluster"] = cluster

        try:
            waiter.wait(**params)
        except DagsterExecutionInterruptedError:
            if self.forward_termination:
                context.log.warning("Dagster process interrupted, terminating ECS tasks")
                self._terminate_tasks(context=context, tasks=tasks, cluster=cluster)
            raise

    def _terminate_tasks(
        self, context: OpExecutionContext, tasks: List[str], cluster: Optional[str] = None
    ):
        for task in tasks:
            try:
                self._client.stop_task(
                    cluster=cluster,
                    task=task,
                    reason="Dagster process terminated",
                )
            except botocore.exceptions.ClientError as e:
                context.log.warning(f"Couldn't stop ECS task {task} in cluster {cluster}:\n{e}")
