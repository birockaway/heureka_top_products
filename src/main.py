"""
Download and write complete list of heureka categories.
Download and write top products for each of them.
"""


import csv
import logging
import os
from typing import List

import requests
import logging_gelf.handlers
import logging_gelf.formatters
from keboola import docker


def get_categories_list(url: str, key: str) -> List[dict]:
    """
    Get complete list of categories with additional category data.
    :param url: heureka api url
    :param key: api access key
    :return: list of dicts with category info
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "category.index",
        "params": {"access_key": key},
    }

    response = requests.post(url=url, json=payload)
    return response.json()["result"]["categories"]


def get_category_detail(url: str, key: str, category_id: int) -> dict:
    """
    Get detailed info per each category including a list of top products.
    :param url: heureka api url
    :param key: api access key
    :param category_id: heureka category identifier
    :return: info per one category
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "category.get",
        "params": {"access_key": key, "id": category_id},
    }

    response = requests.post(url=url, json=payload)
    return response.json()["result"]["category"]


def main():
    logging.basicConfig(level=logging.INFO, handlers=[])
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
            host=os.getenv("KBC_LOGGER_ADDR"), port=int(os.getenv("KBC_LOGGER_PORT"))
        )
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True)
    )
    logger.addHandler(logging_gelf_handler)

    datadir = os.getenv("KBC_DATADIR", "/data/")
    cfg = docker.Config()
    params = cfg.get_parameters()
    logger.info("Extracted parameters.")
    api_url = params.get("api_url")
    api_key = params.get("#api_key")

    categories = get_categories_list(url=api_url, key=api_key)

    # output the category list
    with open(f"{datadir}out/tables/heureka_categories_list.csv", "w") as outfile:
        dict_writer = csv.DictWriter(
            outfile,
            fieldnames=[
                "id",
                "parent_id",
                "name",
                "slug",
                "is_leaf",
                "product_count",
                "url",
            ],
        )
        dict_writer.writeheader()
        dict_writer.writerows(categories)

    logger.info("Written category list.")

    # output individual top products with their category ids
    with open(f"{datadir}out/tables/heureka_top_products.csv", "w") as outfile:
        dict_writer = csv.DictWriter(
            outfile, fieldnames=["id", "name", "slug", "url", "category_id"],
        )
        dict_writer.writeheader()
        for category in categories:
            logger.info(f"Downloading category {category['name']}.")
            category_detail = get_category_detail(
                url=api_url, key=api_key, category_id=int(category["id"])
            )
            top_products = category_detail["top_products"]
            # some categories categories have no top products
            # in their case, nothing gets written here
            for row in top_products:
                row_amended = {"category_id": category["id"], **row}
                dict_writer.writerow(row_amended)

    logger.info("Script completed.")


if __name__ == "__main__":
    main()
