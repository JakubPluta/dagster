# start_asset_marker
import os

# dagster_glue_pipes.py
import boto3
from dagster_aws.pipes import PipesECSClient
from docutils.nodes import entry

from dagster import AssetExecutionContext, asset


@asset
def ecs_pipes_asset(context: AssetExecutionContext, pipes_ecs_client: PipesECSClient):
    return pipes_ecs_client.run(
        context=context,
        task_definition="dagster-pipes:3",
    ).get_materialize_result()


# end_asset_marker

# start_definitions_marker

from dagster import Definitions  # noqa
from dagster_aws.pipes import PipesS3MessageReader


defs = Definitions(
    assets=[ecs_pipes_asset],
    resources={
        "pipes_ecs_client": PipesECSClient(
            client=boto3.client("ecs"),
            message_reader=PipesS3MessageReader(
                client=boto3.client("s3"),
                bucket="aws-glue-assets-467123434025-eu-north-1",
            ),
        )
    },
)

# end_definitions_marker
