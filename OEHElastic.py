#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pprint import pprint
from typing import Literal
from collections import Counter

class OEHElastic:
    es: Elasticsearch

    def __init__(self, hosts=["127.0.0.1"]) -> None:
        self.es = Elasticsearch(hosts=hosts)
        self.last_timestamp = "now-30d" # get values for last 30 days by default

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
            "_source": [
                "nodeRef.id",
                "properties.cclom:title",
                "properties.cm:name"
            ],
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
            "_source": [
                "nodeRef.id",
                "properties.cclom:title",
                "properties.cm:name"
            ],
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
            "_source": [
                "nodeRef.id",
                "properties.cclom:title",
                "properties.cm:name"
            ],
            "size": count,
            "track_total_hits": True
        }
        # print(body)
        return self.es.search(body=body, index="workspace", pretty=True)


    def get_oeh_search_analytics(self, timestamp: str=None, count: int = 1000):
        """
        Returns the oeh search analytics.
        """
        def filter_search_strings(unfiltered: list[dict]):
            for item in unfiltered:
                search_string = item.get("_source", {}).get("searchString", None)
                if search_string:
                    yield search_string
                else:
                    continue


        if not timestamp:
            timestamp = self.last_timestamp
        body = {
            "query": { 
                "range": { 
                "timestamp": { 
                    "gt": timestamp
                } 
                } 
            },
            "size": count,
            "sort": [
                {
                "timestamp": {
                    "order": "desc"
                   }
                }
            ]
        }

        r: list[dict] = self.es.search(body=body, index="oeh-search-analytics", pretty=True).get("hits", {}).get("hits", [])
        if len(r):
            self.last_timestamp = r[0].get("_source", {}).get("timestamp")
        filtered_search_strings = filter_search_strings(r)
        search_counter = Counter(list(filtered_search_strings))
        return search_counter

if __name__ == "__main__":
    oeh = OEHElastic()
    print("\n\n\n\n")
    # print(json.dumps(oeh.getStatisicCounts("4940d5da-9b21-4ec0-8824-d16e0409e629"), indent=4))
    # print(json.dumps(oeh.get_material_by_condition("4940d5da-9b21-4ec0-8824-d16e0409e629", count=0), indent=4))
# ohne titel
    # print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:commonlicense_key.keyword", 0), indent=4))
    pprint(oeh.get_oeh_search_analytics())
    pprint(oeh.get_oeh_search_analytics())
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
