class Constants:

    ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON = "/json/list/{0}"
    ENDPOINT_SAX_LIST_PIPELINES = "/getPipelineNames/list?workSpaceName={0}"
    ENDPOINT_SAX_GET_PIPELINE_STATUS_JSON = "/pipeline/detail?pipelineName={0}"
    ENDPOINT_SAX_START_PIPELINE = "/subsystem/action?name={0}&engine=spark&action=start"
    ENDPOINT_SAX_KILL_PIPELINE = "/subsystem/action?name={0}&engine=spark&action=kill"
    ENDPOINT_SAX_UPDATE_PIPELINE_JSON = '/subsystem/update/json/{0}'

