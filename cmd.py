import easyargs
import json
import requests
import logger
from concurrent.futures import ThreadPoolExecutor

default_index_url = "https://api.nuget.org/v3/index.json"


def process_resource(resource):

@easyargs
def main(index_url=default_index_url, threads=16) -> int:
    r = requests.get(index_url)
    index = r.json()
    if not "resources" in index:
        logger.error(f"No 'resources' in {index}")
        return -1

    resources = index["resources"]
    with ThreadPoolExecutor(max_workers = 5) as executor:
        for resource in resources:
            process_resource(resource)

        futures = {executor.submit(process_resource, resource): resource for resource in resources}
        executor.wait()

        results = {future.result() for future in futures}

    print(f"{results}")


if __name__ == "__main__":
    sys.exit(main())