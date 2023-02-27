import json
import sys


class ConfigReader:
    def __init__(self, logger, config_file_path):
        self.logger = logger
        try:
            with open(config_file_path, "rb") as conf_file:
                self.config_data = json.load(conf_file)
                self.logger.debug(self.config_data)
            self.logger.info("Configuration loaded, File = {}".format(conf_file))
        except Exception as e:
            print("There is some Error while loading {} file please check logs for more details"
                  .format(config_file_path))
            self.logger.error("Config file {0} is not in right format - {1}".format(config_file_path, str(e)))
            sys.exit(-1)

    def get_config_value(self, key):
        try:
            return self.config_data[key]
        except Exception as e:
            self.logger.error("Key = {0} not found in configuration {1}".format(key, self.config_data))
            self.logger.error("Error : {}".format(str(e)))
            return None

