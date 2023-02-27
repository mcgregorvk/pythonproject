import requests
import uuid
import json


class RestApiManager:
    def __init__(self, logger, api_url, token=None):
        self.logger = logger
        self.logger.info("Initialized RestApi Manager")
        self.api_url = api_url
        self.token = None
        if token is not None:
            self.token = token

    def get_url(self, endpoint):
        return self.api_url + endpoint

    def mask_url(self, url):
        host_name = url.split(':')[1].split('//')[1]
        return url.replace(host_name, '*******')

    def get_api_response(self, endpoint):
        url = self.get_url(endpoint)
        if self.token is not None:
            self.logger.info("Request Url -{0}".format(self.mask_url(url)))
            result = requests.get(url, headers={'token': self.token})
        else:
            self.logger.info("Request Url -{0}".format(self.mask_url(url)))
            result = requests.get(url)

        if result.status_code != 200:
            self.logger.error(
                'GET request url -{0}, status code- {1}'.format(self.mask_url(url), result.status_code))
            return None

        return result.json()

    def post_api_response(self, endpoint, payload=None):
        is_operation_valid = False
        url = self.get_url(endpoint)
        try:
            header = {
                'token': self.token,
                'cache-control': "no-cache",
                'pipeline-monitor-token': str(uuid.uuid4())
            }
            self.logger.debug("Request headers are -{0}".format(header))
            if self.token is not None and payload is not None:
                self.logger.info("Request Url -{0}".format(self.mask_url(url)))
                result = requests.request("POST", url, headers=header, data=json.dumps(payload))
            elif self.token is not None:
                self.logger.info("Request Url -{0}".format(self.mask_url(url)))
                result = requests.request("POST", url, headers=header)
            else:
                result = requests.request("POST", url)
            result_output = result.json()
            if result.status_code == 200 and result_output is not None and result_output['status'] == "SUCCESS":
                self.logger.info(
                    'Post request -{0}  StatusCode-{1} responseContent-{2}'.format(
                        self.mask_url(url),
                        result.status_code,
                        result.content))
                is_operation_valid = True
            else:
                self.logger.error(
                    'Error in Post request -{0}  StatusCode-{1} Response-{2}'.format(
                        self.mask_url(url),
                        result.status_code,
                        result_output))
            return is_operation_valid
        except(ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return is_operation_valid

    def is_json_element_exist_cp(self, data, value, custom_processor_label):
        is_exist = False
        try:
            if data['config'][value] is not None:
                if data['label'] == custom_processor_label:
                    self.logger.info('Custom Processor exists.')
                    if data['config'][value]['STORAGE_TYPE']:
                        self.logger.info('Custom Processor Json element exists.')
                        is_exist = True
            return is_exist
        except (ValueError, Exception):
            return is_exist

