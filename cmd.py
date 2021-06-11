import sys
import easyargs
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, wait

ID_KEY = "@id"

logger = logging.getLogger()
def process_resource(resource):
    resource_type = resource.get("@type", "")
    if resource_type.startswith("SearchAutocompleteService"):
        logger.info(f"Skip autocomplete {resource}")
        return None

    id = resource.get(ID_KEY, None)
    if id is None:
        logger.error(f"{resource} is missing ID")
        return None

    try:
        r = requests.get(id)
        index = r.json()
    except Exception:
        logger.exception(f"Failed to fetch {id}")
        return None

    if "data" not in index:
        logger.error(f"'data' is missing in {resource}/{index}")
        return None

    data = index["data"]
    packages = {}
    for d in data:
        if type(d) is not dict:
            logger.error(f"{d} in {resource}/{index}/{data} is not a dictionary")
            continue

        package_id = d.get(ID_KEY, None)
        if package_id is None:
            logger.error(f"{resource}/{index}/{data} is missing ID")
            continue
        versions = {version.get(ID_KEY, None) for version in d.get("versions", [])}
        packages[package_id] = versions

    return packages

@easyargs
def main(command, index_url='https://api.nuget.org/v3/index.json', max_threads=16):
    commands = ["hash", "list"]
    if command not in commands:
        logger.error(f"{command} is not from {commands}")
        return -1

    r = requests.get(index_url)
    index = r.json()
    if "resources" not in index:
        logger.error(f"No 'resources' in {index}")
        return -2

    resources = index["resources"]
    with ThreadPoolExecutor(max_workers = max_threads) as executor:
        for resource in resources:
            process_resource(resource)

        futures = {executor.submit(process_resource, resource): resource for resource in resources}
        wait(futures)

        results = {}
        for future in futures:
            result = future.result()
            if result is None:
                continue

            results.update(result)

    results_json = json.dumps(results, sort_keys=True, indent=2)
    print(f"{results_json}")

if __name__ == '__main__':
    main()