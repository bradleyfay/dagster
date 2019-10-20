import os

from dagster import check
from dagster.core.types import Dict, Field, String, configurable_plugin


@configurable_plugin(Dict({'base_dir': Field(String)}))
def local_artifact_storage_configurable_plugin(plugin_config):
    return LocalArtifactStorage(base_dir=plugin_config['base_dir'])


class LocalArtifactStorage:
    def __init__(self, base_dir):
        self._base_dir = base_dir

    @property
    def base_dir(self):
        return self._base_dir

    def file_manager_dir(self, run_id):
        check.str_param(run_id, 'run_id')
        return os.path.join(self.base_dir, 'storage', run_id, 'files')

    def intermediates_dir(self, run_id):
        return os.path.join(self.base_dir, 'storage', run_id, '')

    @property
    def schedules_dir(self):
        return os.path.join(self.base_dir, 'schedules')
