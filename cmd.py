import sys
import easyargs
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, wait

logger = logging.getLogger()

def process_resource(resource):
    return resource.get("@id", None)

@easyargs
def main(command, index_url='https://api.nuget.org/v3/index.json', threads=16):
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
    with ThreadPoolExecutor(max_workers = 5) as executor:
        for resource in resources:
            process_resource(resource)

        futures = {executor.submit(process_resource, resource): resource for resource in resources}
        wait(futures)

        results = set()
        for future in futures:
            result = future.result()
            if result is None:
                logger.error(f"{resource} is missing ID")
                continue

            results.add(result)

    print(f"{results}")

if __name__ == '__main__':
    main()