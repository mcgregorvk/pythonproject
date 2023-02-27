import sys
import logging
from logging import handlers
from ConfigReader import ConfigReader
from RestApiModule import RestApiManager
from ConstantsModule import Constants
from datetime import datetime
import time


class StopStartPipeline:
    def __init__(self):
        try:
            self.logger = logging.getLogger(__name__)
            self.config_manager = ConfigReader(self.logger, config_file_path="config.json")
            # Logger configuration
            filename = self.config_manager.get_config_value("log_file_name")
            log_level = self.config_manager.get_config_value("log_level")
            log_formatter = self.config_manager.get_config_value("log_formatter")
            log_max_size_mb = int(self.config_manager.get_config_value("log_max_size_mb"))
            log_file_backup_counts = int(self.config_manager.get_config_value("log_file_backup_counts"))
            formatter = logging.Formatter(log_formatter)
            log_handler = handlers.RotatingFileHandler(filename, mode='a', maxBytes=log_max_size_mb * 1024 * 1024,
                                                       backupCount=log_file_backup_counts, encoding='utf-8',
                                                       delay=False)
            log_handler.setLevel(log_level.upper())
            log_handler.setFormatter(formatter)
            self.logger.setLevel(log_level.upper())
            self.logger.addHandler(log_handler)

            self.logger.info("Starting pipeline stop/start script")
            self.sax_api_url = self.config_manager.get_config_value("sax_api_url")
            self.sax_api_token = self.config_manager.get_config_value("sax_api_token")
            self.sax_user = self.config_manager.get_config_value("sax_user")
            self.sax_pipelines = self.config_manager.get_config_value("sax_pipelines")
            self.sax_pipelines_selective_stop = self.config_manager.get_config_value("sax_pipelines_selective_stop")
            self.retryCount = int(self.config_manager.get_config_value("retryCount"))
            self.retrySleep_inSec = int(self.config_manager.get_config_value("retrySleep_inSec"))
            self.logger.info("config read properly")
            self.sax_api_manager = RestApiManager(self.logger, self.sax_api_url, token=self.sax_api_token)
        except Exception as e:
            print("There is some error please check logs for more details")
            self.logger.error("Error Initializing StopStartPipeline object : " + str(e))
            sys.exit(-1)

    def get_pipeline_details(self, pipeline_name):
        try:
            pipeline_detail_json = self.sax_api_manager.get_api_response(
                Constants.ENDPOINT_SAX_GET_PIPELINE_STATUS_JSON.format(pipeline_name))
            return True, pipeline_detail_json
        except Exception as e:
            self.logger.error("Error getting pipeline details: "+str(e))
            return False, None

    def check_pipeline_status(self, pipeline_detail_json):
        return pipeline_detail_json['spark']['pipelines'][0]['status']

    def stop_pipeline(self, pipeline_name):
        # kill pipeline, if unable to kill pipeline after retries then exit script with exception message
        t1 = datetime.now()
        is_valid = False
        retry_count = self.retryCount
        while retry_count >= 1:
            self.logger.info("Pipeline kill attempt- {0}".format(str((self.retryCount + 1) - retry_count)))
            is_pipeline_killed = self.sax_api_manager.post_api_response(
                Constants.ENDPOINT_SAX_KILL_PIPELINE.format(pipeline_name))
            self.logger.info("Pipeline kill status - {0}".format(str(is_pipeline_killed)))
            if is_pipeline_killed:
                flag_details, pipeline_status_json = self.get_pipeline_details(pipeline_name)
                if not flag_details:
                    break
                self.logger.debug("pipeline_status_json - {0}".format(pipeline_status_json))
                status = self.check_pipeline_status(pipeline_status_json)
                self.logger.debug("current status of pipeline: " + str(status))
                if pipeline_status_json is not None and status == "STOPPED":
                    self.logger.info("Pipeline :{} has been stopped.".format(pipeline_name))
                    is_valid = True
            if is_valid:
                break
            retry_count = retry_count - 1
            time.sleep(self.retrySleep_inSec)
        if not is_valid:
            self.logger.error("API is not able to stop the pipeline: {0}.".format(pipeline_name))
            raise Exception("API is not able to stop the pipeline")
        t2 = datetime.now()
        self.logger.info("Time taken to kill pipeline: " + str(t2 - t1))
        return is_valid

    def get_pipeline_configurations(self, pipeline_name):
        return self.sax_api_manager.get_api_response(
            Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))

    def send_update_json_request(self, pipeline_config_json, pipeline_name):
        is_valid = self.sax_api_manager.post_api_response(Constants.ENDPOINT_SAX_UPDATE_PIPELINE_JSON.format(
            pipeline_name), pipeline_config_json)
        if is_valid:
            self.logger.info('Successfully updated the pipeline Json.')
        else:
            self.logger.error('Failed to update the pipeline json.')
        return is_valid
    def start_pipeline(self, pipeline_name):
        is_valid = False
        try:
            is_valid = self.sax_api_manager.post_api_response(
                Constants.ENDPOINT_SAX_START_PIPELINE.format(pipeline_name))
            if is_valid:
                self.logger.info("Pipeline -{} has been started.".format(pipeline_name))
            else:
                self.logger.error("Pipeline -{} failed to start.".format(pipeline_name))
            return is_valid
        except (ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return is_valid

    def get_all_pipelines_as_list(self):
        try:
            all_pipeline_list = self.sax_api_manager.get_api_response(
                Constants.ENDPOINT_SAX_LIST_PIPELINES.format(self.sax_user))
            return True, all_pipeline_list
        except Exception as e:
            self.logger.error("Error getting all pipeline from sax: " + str(e))
            return False, None
def main():
    try:
        print("Initializing StopStartPipeline Script")
        pip_obj = StopStartPipeline()
        
        action = sys.argv[1]
        # get list of all pipelines present on sax
        flag_pip, all_pipelines = pip_obj.get_all_pipelines_as_list()
        if not flag_pip:
            print("There is some error please check logs for more details")
            pip_obj.logger.warning("exiting")
            sys.exit(-1)

        # get all pipelines matching prefixes
        pipeline_prefix_map = {}
        final_pipelines = []
        for pipelines in pip_obj.sax_pipelines:
            pip_list = []
            all_pipeline_list = all_pipelines
            for prefix in pipelines["pipeline_prefix"]:
                pip_list = [s for s in all_pipeline_list if prefix.lower() in s.lower()]
                all_pipeline_list = pip_list
            if len(pip_list) <= 0:
                pip_obj.logger.info("No pipelines found for prefixes: " + str(pipelines["pipeline_prefix"]))
            else:
                for name in pip_list:
                    pipeline_prefix_map[name] = ";".join(pipelines["pipeline_prefix"])
                final_pipelines = final_pipelines + pip_list
        pip_obj.logger.info("list of pipelines that are matched with prefix: " + str(final_pipelines))
        if sax_pipelines_selective_stop:true
        final_pipelines
        if sax_pipelines_selective_stop:false
        final_pipelines = all_pipelines
        for pipeline in final_pipelines:
            try:
                if action == "stop" :
                    pip_obj.stop_pipeline(pipeline)
                    
                # update custom processor storage type to "HYBRID"
                
                elif action == "start" :
                # start main pipeline
                    if not pip_obj.start_pipeline(pipeline):
                        print("There is some error in pipeline " + pipeline +
                          " please check logs for more details")
                        pip_obj.logger.error("skipping as error in starting pipeline " + pipeline)
                        continue

            except Exception as e:
                print("There is some error in pipeline " + pipeline + " please check logs for more details")
                pip_obj.logger.error("error in pipeline " + pipeline + ": " + str(e))
                continue


        print("Pipelines update and restart done ending")
        pip_obj.logger.info("Pipelines update and restart done ending")
    except Exception as e:
        print("Error in starting script: " + str(e))


if __name__ == '__main__':
    main()


