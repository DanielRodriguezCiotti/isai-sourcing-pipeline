from prefect import flow

from src.tasks import retrieve_all_filter_values


@flow(name="compute-search-resources-flow")
def compute_search_resources_flow():
    retrieve_all_filter_values()
