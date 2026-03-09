from sensate.schema.config_schema import GlobalConfigSchema

class GlobalConfig():
    """Global configuration for the Sensate project."""
    def init(self, config: GlobalConfigSchema):
        self.config = config
        
    def get_dataset_name(self) -> str:
        return self.config.dataset_name

global_config = GlobalConfig()