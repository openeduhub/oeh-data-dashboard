#!/usr/bin/env python3

from dataclasses import dataclass, field
import logging

from elasticsearch import Elasticsearch
from pprint import pprint
from typing import Generator, Literal
from collections import Counter, defaultdict, namedtuple
import requests

logging.basicConfig(level=logging.INFO)

# TODO call this from Collections.py
class EduSharing:
    @staticmethod
    def get_collections():
        ES_COLLECTIONS_URL = "https://redaktion.openeduhub.net/edu-sharing/rest/collection/v1/collections/local/5e40e372-735c-4b17-bbf7-e827a5702b57/children/collections?scope=TYPE_EDITORIAL&skipCount=0&maxItems=1247483647&sortProperties=cm%3Acreated&sortAscending=true&"

        headers = {
            "Accept": "application/json"
        }

        params = {
            "scope": "TYPE_EDITORIAL",
            "skipCount": "0",
            "maxItems": "1247483647",
            "sortProperties": "cm%3Acreated",
            "sortAscending": "true"
        }

        r_collections: list = requests.get(
            ES_COLLECTIONS_URL,
            headers=headers,
            params=params
        ).json().get("collections")

        # if sort:
        #     collections = sorted([Collection(item) for item in r_collections])
        #     return collections
        return r_collections


@dataclass
class SearchedMaterialInfo:
    _id: str
    search_strings: Counter
    clicks: int


class OEHElastic:
    es: Elasticsearch

    def __init__(self, hosts=["127.0.0.1"]) -> None:
        self.es = Elasticsearch(hosts=hosts)
        self.last_timestamp = "now-30d" # get values for last 30 days by default
        self.searched_materials_by_collection = self.get_oeh_search_analytics()

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


    def get_oeh_search_analytics(self, timestamp: str=None, count: int = 500):
        """
        Returns the oeh search analytics.
        """
        def filter_search_strings(unfiltered: list[dict]) -> Generator:
            for item in unfiltered:
                search_string = item.get("_source", {}).get("searchString", None)
                if search_string and search_string.strip() != "":
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
  
        filtered_search_strings = filter_search_strings(r)
        search_counter = Counter(list(filtered_search_strings))
        @dataclass
        class ResourceData:
            search_strings: list = field(default_factory=list)
            fps: set = field(default_factory=set)
            clicks: int = 0


        def filter_for_terms_and_materials(res: list[dict]):
            terms_and_materials = defaultdict(list)
            materials_by_terms = defaultdict(ResourceData)
            filtered_res = (item for item in res if item.get("_source", {}).get("action", None) == "result_click")
            for item in (item.get("_source") for item in filtered_res):
                search_string = item.get("searchString")
                clicked_resource = item.get("clickedResult").get("id")
                terms_and_materials[search_string].append(clicked_resource)

                # we got to check the FPs for the given resource
                logging.info(f"checking included fps for resource id: {clicked_resource}")
                included_fps = self.check_resource_in_fps(clicked_resource, list(collections_ids_title.keys()))
                materials_by_terms[clicked_resource]
                materials_by_terms[clicked_resource].search_strings.append(search_string)
                materials_by_terms[clicked_resource].fps.update(included_fps)
                materials_by_terms[clicked_resource].clicks += 1
            return terms_and_materials, materials_by_terms


        # we have to check if path contains one of the edu-sharing collections with an elastic query
        # get fpm collections
        collections = EduSharing.get_collections()
        collections_ids_title = {item.get("properties").get("sys:node-uuid")[0]: item.get("title") for item in collections}
        terms_and_materials, materials_by_terms = filter_for_terms_and_materials(r)

        # assign material to fpm portals
        collections_by_material = defaultdict(list)
        for key in materials_by_terms:
            if fps:=materials_by_terms[key].fps:
                for fp in fps:
                    material = SearchedMaterialInfo(key, Counter(materials_by_terms[key].search_strings), materials_by_terms[key].clicks)
                    collections_by_material[fp].append(material)


        return collections_by_material

    def get_node_path(self, node_id) -> dict:
        """
        Queries elastic for a given node and returns the collection paths
        """
        
        body = {
            "query": { 
                "match": {
                     "nodeRef.id": node_id
                }
            },
            "_source": "collections.path"
        }
        return self.es.search(body=body, index="workspace", pretty=True)


    def check_resource_in_fps(self, resource_id: str, collection_ids: list) -> list:
        try:
            paths = self.get_node_path(resource_id).get("hits", {}).get("hits", [])[0].get("_source").get("collections", [])[0].get("path", None)
            included_fps = [path for path in paths if path in collection_ids]
            return included_fps
        except:
            return []

    
    

if __name__ == "__main__":
    oeh = OEHElastic()
    print("\n\n\n\n")
    # print(json.dumps(oeh.getStatisicCounts("4940d5da-9b21-4ec0-8824-d16e0409e629"), indent=4))
    # print(json.dumps(oeh.get_material_by_condition("4940d5da-9b21-4ec0-8824-d16e0409e629", count=0), indent=4))
# ohne titel
    # print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:commonlicense_key.keyword", 0), indent=4))
    # pprint(oeh.get_oeh_search_analytics())
    oeh.get_oeh_search_analytics()
    # ohne fachzuordnung
    # print(json.dumps(oeh.getMaterialByMissingAttribute("15fce411-54d9-467f-8f35-61ea374a298d", "properties.ccm:educationalcontext", 10), indent=4))
# # ohne fachzuordnung
# print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:taxonid", 0), indent=4))
# # ohne schlagworte
# print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.cclom:general_keyword", 0), indent=4))

# # sammlung ohne beschreibung
# print(json.dumps(oeh.getCollectionByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.cclom:general_keyword", 0), indent=4))
# # sammlung ohne schlagworte
    # print(json.dumps(oeh.getCollectionByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.cm:description", 10), indent=4))
