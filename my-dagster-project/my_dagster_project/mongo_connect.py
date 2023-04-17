from dagster import resource, op, job
from pymongo import MongoClient
from pydantic import BaseSettings, AnyUrl


class MongoDsn(AnyUrl):
    allowed_schemes = {"mongodb", "mongodb+srv"}


class Settings(BaseSettings):
    """https://pydantic-docs.helpmanual.io/usage/settings/"""

    # auth_key: str
    # api_key: str = Field(..., env='my_api_key')

    uri: MongoDsn = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.8.0"

    class Config:
        env_file = "conf.f"
        env_file_encoding = "utf-8"


@resource(config_schema={"uri": str})
def mongo_test(context):
    """https://docs.dagster.io/concepts/resources#resource-configuration
    """
    uri = context.resource_config["uri"]
    client = MongoClient(uri)
    return client.test


@resource(config_schema={"uri": str})
def mongo_prod(context):
    uri = context.resource_config["uri"]
    client = MongoClient(uri)
    return client.prod


@op(required_resource_keys={"mongo"})
def my_op(context):
    data = {
        "item": "canvas",
        "qty": 100,
        "tags": ["cotton"],
        "size": {"h": 28, "w": 35.5, "uom": "cm"},
    }
    context.resources.mongo.inventory.insert_one(data)


@job(resource_defs={"mongo": mongo_test})
def test_job():
    my_op()


if __name__ == "__main__":
    debug_settings = Settings()
    result = test_job.execute_in_process(
        run_config={"resources": {"mongo": {"config": {"uri": debug_settings.uri}}}}
    )