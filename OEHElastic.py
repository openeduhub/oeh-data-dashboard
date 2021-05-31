#!/usr/bin/env python3

import json
from typing import Literal
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

SOURCE_FIELDS = [
    "nodeRef",
    "type",
    "preview",
    "properties.cclom:title",
    "properties.cm:name"
]
class OEHElastic:

    es: Elasticsearch

    def __init__(self, hosts=[os.getenv("ES_HOST", "localhost")]) -> None:
        self.es = Elasticsearch(hosts=hosts)

    def getBaseCondition(self, collection_id: str, additional_must: dict = None) -> dict:
        must_conditions = [
            {"terms": {"type": ['ccm:io']}},
            {"terms": {"permissions.read": ['GROUP_EVERYONE']}},
            {"bool" : {
                "should": [
                    {"match": {"collections.path": collection_id }},
                    {"match": {"collections.nodeRef.id": collection_id}},
                ],
                "minimum_should_match": 1
            }
            },
            {"terms": {"properties.cm:edu_metadataset": ['mds_oeh']}},
            {"terms": {"nodeRef.storeRef.protocol": ['workspace']}},
        ]
        if additional_must:
            must_conditions.append(additional_must)
        return {
            "bool": {
                "must": must_conditions
            }
        }

    def getCollectionByMissingAttribute(self, collection_id: str, attribute: str, count: int=10000) -> dict:
        """
        Returns an es-query-result with collections that have a given missing attribute.
        If count is set to 0, only the total number will be returned.
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"type": ['ccm:map']}},
                        {"terms": {"permissions.read": ['GROUP_EVERYONE']}},
                        {"bool" : {
                            "should": [
                                {"match": {"path": collection_id }},
                                {"match": {"nodeRef.id": collection_id}}
                            ],
                            "minimum_should_match": 1
                        }
                        },
                    ],
                    "must_not": [{"wildcard": {attribute: "*"}}]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": count,
            "track_total_hits": True
        }
        # print(body)
        return self.es.search(body=body, index="workspace", pretty=True)


    def getMaterialByMissingAttribute(self, collection_id: str, attribute: str, count: int=10000) -> dict:
        """
        Returns the es-query result for a given collection_id and the attribute.
        If count is set to 0, just the total number will be returned in the es-query-result.
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(collection_id),
                    ],
                    "must_not": [{"wildcard": {attribute: "*"}}]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": count,
            "track_total_hits": True
        }
        # pprint(body)
        return self.es.search(body=body, index="workspace", pretty=True)

    def getStatisicCounts(self, collection_id: str, attribute: str="properties.ccm:commonlicense_key.keyword") -> dict:
        """
        Returns count of values for a given attribute (default: license)
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(collection_id),
                    ]
                }
            },
            "aggs": {
                "license": {
                    "terms": {
                        "field": attribute,
                    }
                }
            },
            "size": 0,
            "track_total_hits": True
        }
        # print(body)
        return self.es.search(body=body, index="workspace", pretty=True)


    def get_material_by_condition(self, collection_id: str, condition: Literal["missing_license"] = None, count=10000) -> dict:
        """
        Returns count of values for a given attribute (default: license)
        """
        if condition == "missing_license":
            additional_condition = {
                "terms": {
                    "properties.ccm:commonlicense_key.keyword": [ "NONE", "", "UNTERRICHTS_UND_LEHRMEDIEN"]
                }
            }
        else:
            additional_condition = None
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(collection_id, additional_condition),
                    ]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": count,
            "track_total_hits": True
        }
        # print(body)
        return self.es.search(body=body, index="workspace", pretty=True)

if __name__ == "__main__":
    oeh = OEHElastic()
    print("\n\n\n\n")
    # print(json.dumps(oeh.getStatisicCounts("4940d5da-9b21-4ec0-8824-d16e0409e629"), indent=4))
    # print(json.dumps(oeh.get_material_by_condition("4940d5da-9b21-4ec0-8824-d16e0409e629", count=0), indent=4))
    # ohne titel
    print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:commonlicense_key.keyword", 0), indent=4))
    # ohne fachzuordnung
    # print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:educationalcontext", 10), indent=4))
# # ohne fachzuordnung
# print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:taxonid", 0), indent=4))
# # ohne schlagworte
# print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.cclom:general_keyword", 0), indent=4))

# # sammlung ohne beschreibung
# print(json.dumps(oeh.getCollectionByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.cclom:general_keyword", 0), indent=4))
# # sammlung ohne schlagworte
# print(json.dumps(oeh.getCollectionByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.cm:description", 10), indent=4))
