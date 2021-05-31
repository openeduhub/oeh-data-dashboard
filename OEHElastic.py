#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from pprint import pprint
from typing import Generator, Literal
from collections import Counter, defaultdict
import requests

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

        # TODO
        # collections = sorted([Collection(item) for item in r_collections])

        return r_collections


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


    def get_oeh_search_analytics(self, timestamp: str=None, count: int = 10000):
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

        # which material is associated with which term and to which portal does it belong?
        # action result_click
    #     {
    #     "_index" : "oeh-search-analytics",
    #     "_type" : "_doc",
    #     "_id" : "m-NmsnkBlIJJNA7cpCRV",
    #     "_score" : null,
    #     "_source" : {
    #       "action" : "result_click",
    #       "sessionId" : "g7k9o7iq8dkp85ggqa",
    #       "userAgent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    #       "screenWidth" : 1920,
    #       "screenHeight" : 1080,
    #       "language" : "en",
    #       "searchString" : "parabel",
    #       "page" : 0,
    #       "filters" : { },
    #       "filtersSidebarIsVisible" : false,
    #       "clickedResult" : {
    #         "id" : "6987b8c5-2469-4854-bb0b-efe3423e30b9",
    #         "lom" : {
    #           "general" : {
    #             "title" : "Parabel und Parabel",
    #             "keyword" : null
    #           },
    #           "technical" : {
    #             "location" : "https://www.geogebra.org/m/sMw85hmS"
    #           }
    #         },
    #         "type" : "content",
    #         "source" : {
    #           "name" : "GeoGebra",
    #           "url" : "https://www.geogebra.org"
    #         },
    #         "license" : {
    #           "oer" : true
    #         },
    #         "editorialTags" : [ ],
    #         "skos" : {
    #           "discipline" : null,
    #           "educationalContext" : null,
    #           "learningResourceType" : [
    #             {
    #               "id" : "http://w3id.org/openeduhub/vocabs/learningResourceType/worksheet",
    #               "label" : "worksheet"
    #             }
    #           ]
    #         }
    #       },
    #       "clickKind" : "click",
    #       "timestamp" : "2021-05-28T09:55:41.760Z"
    #     },
    #     "sort" : [
    #       1622195741760
    #     ]
    #   },
        # dict of {term: [clicked Materials]}
        def filter_for_terms_and_materials(res: list[dict]):
            terms_and_materials = defaultdict(list)
            filtered_res = (item for item in res if item.get("_source", {}).get("action", None) == "result_click")
            for item in (item.get("_source") for item in filtered_res):
                terms_and_materials[item.get("searchString")].append(item.get("clickedResult").get("id"))
            return terms_and_materials


        terms_and_materials = filter_for_terms_and_materials(r)
        # we have to check if path contains one of the edu-sharing collections with an elastic query
        # get fpm collections
        collections = EduSharing.get_collections()
        collections_ids_title = {item.get("properties").get("sys:node-uuid")[0]: item.get("title") for item in collections}

        return search_counter

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
