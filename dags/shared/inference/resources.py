from airflow.sdk import Variable


def get_resources() -> dict[str, str]:
    return {
        "INFERENCE_API_URL": Variable.get("INFERENCE_API_URL"),
        "INFERENCE_API_KEY": Variable.get("INFERENCE_API_KEY"),
        "INFERENCE_ENDPOINT_DECISION": Variable.get("INFERENCE_ENDPOINT_DECISION"),
        "INFERENCE_JSS_AGENT_ID": Variable.get("INFERENCE_JSS_AGENT_ID"),
    }
