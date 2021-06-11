import sys
import easyargs
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, wait

ID_KEY = "@id"
TYPE_KEY = "@type"

logger = logging.getLogger()

def get_json(url):
    try:
        r = requests.get(url)
    except Exception:
        logger.exception(f"Failed to fetch {url}")
        return None

    if r.status_code != 200:
        logger.error(f"Response {r.status_code} for {url}")
        return None

    try:
        data = r.json()
    except Exception:
        logger.exception(f"Failed to unmarchal {url}")
        return None

    return data

def process_catalogue_page(catalogue_page):
    catalogue = get_json(catalogue_page)
    if catalogue == None:
        return None

    items = catalogue.get("items", None)
    if items is None:
        logger.error(f"{catalogue} is missing items")
        return None

    packages = {}
    for item in items:
        catalogue_type = resource.get(TYPE_KEY, "")
        if catalogue_type != "nuget:PackageDetails":
            continue
        id = resource.get(ID_KEY, None)
        if id is None:
            logger.error(f"{catalogue} is missing ID")
            continue
        item_data = get_json(id)
        if item_data == None:
            continue
        package_id = item_data.get("id", None)
        if package_id is None:
            logger.error(f"{item_data} is missing ID")
            continue
        packages[package_id] = []

    return packages

def process_catalogue(catalogue):
    items = catalogue.get("items", None)
    if items is None:
        return None

    all_packages = {}
    for item in items:
        catalogue_type = resource.get(TYPE_KEY, "")
        if catalogue_type != "CatalogPage":
            logger.error(f"Skip {catalogue_type} in {catalogue}")
            continue
        id = resource.get(ID_KEY, None)
        if id is None:
            logger.error(f"{catalogue} is missing ID")
            continue
        packages = process_catalogue_page(id)
        all_packages.update(packages)

    return all_packages

def process_resource_package(resource):

def process_resource(resource):
    resource_type = resource.get(TYPE_KEY, "")
    if resource_type.startswith("SearchAutocompleteService"):
        logger.info(f"Skip autocomplete {resource}")
        return None

    if resource_type.startswith("Catalog"):
        logger.info(f"{resource} is a catalogue")
        return process_catalogue(resource)

    id = resource.get(ID_KEY, None)
    if id is None:
        logger.error(f"{resource} is missing ID")
        return None

    index = get_json(id)
    if index == None:
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

        package_id = d.get("id", None)
        if package_id is None:
            logger.error(f"{resource}/{index}/{data} is missing ID")
            continue
        versions = [version.get(ID_KEY, None) for version in d.get("versions", [])]
        packages[package_id] = versions

    logger.info(f"Collected {len(packages)} packages")
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
    packages = {}
    with ThreadPoolExecutor(max_workers = max_threads) as executor:
        for resource in resources:
            process_resource(resource)

        futures = {executor.submit(process_resource, resource): resource for resource in resources}
        wait(futures)

        for future in futures:
            result = future.result()
            if result is None:
                continue

            packages.update(result)


    logger.info(f"Collected Total {len(packages)} packages")
    packages_json = json.dumps(packages, sort_keys=True, indent=2)
    if command == "list":
        print(f"{packages}")
        return

if __name__ == '__main__':
    main()