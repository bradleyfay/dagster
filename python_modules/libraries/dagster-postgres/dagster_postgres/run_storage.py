from dagster.core.storage.runs import RunStorageSQLMetadata, SQLRunStorage, create_engine
from dagster.core.types import Dict, Field, String, configurable_plugin


@configurable_plugin(Dict({'postgres_url': Field(String)}))
def postgres_run_storage_configurable_plugin(plugin_config):
    return PostgresRunStorage(postgres_url=plugin_config['postgres_url'])


class PostgresRunStorage(SQLRunStorage):
    def __init__(self, postgres_url):
        self.engine = create_engine(postgres_url)
        RunStorageSQLMetadata.create_all(self.engine)

    @staticmethod
    def create_clean_storage(postgres_url):
        engine = create_engine(postgres_url)
        RunStorageSQLMetadata.drop_all(engine)
        return PostgresRunStorage(postgres_url)

    def connect(self):
        return self.engine.connect()
