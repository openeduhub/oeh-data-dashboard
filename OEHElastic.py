#!/usr/bin/env python3

import json
import logging
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Generator, Literal

import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, client

load_dotenv()

logging.basicConfig(level=logging.INFO)

ES_PREVIEW_URL = "https://redaktion.openeduhub.net/edu-sharing/preview?maxWidth=200&maxHeight=200&crop=true&storeProtocol=workspace&storeId=SpacesStore&nodeId={}"

@dataclass
class Bucket:
    key: str
    doc_count: int

    def as_dict(self):
        return {
            "key": self.key,
            "doc_count": self.doc_count
        }

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

        return r_collections


@dataclass
class SearchedMaterialInfo:
    _id: str = ""
    search_strings: Counter = field(default_factory=Counter)
    clicks: int = 0
    name: str = ""
    title: str = ""
    crawler: str = ""
    fps: set = field(default_factory=set)


    def as_dict(self):
        search_term_count = "Suchbegriff: \"{}\" ({})" # term, count
        return {
            "id": self._id,
            "search_strings": " ".join([search_term_count.format(term, count) for term, count in self.search_strings.items()]),
            "clicks": self.clicks,
            "name": self.name,
            "title": self.title,
            "crawler": self.crawler,
            "thumbnail_url": ES_PREVIEW_URL.format(self._id)
            # "fps": self.fps
        }


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
        self.last_timestamp = "now-30d" # get values for last 30 days by default
        self.searched_materials_by_collection = {} # TODO maybe we can use a @property.setter method here?
        
        self.get_oeh_search_analytics(timestamp = self.last_timestamp, count=1000)

    def getBaseCondition(self, collection_id: str = None, additional_must: dict = None) -> dict:
        must_conditions = [
            {"terms": {"type": ['ccm:io']}},
            {"terms": {"permissions.read": ['GROUP_EVERYONE']}},
            {"terms": {"properties.cm:edu_metadataset": ['mds_oeh']}},
            {"terms": {"nodeRef.storeRef.protocol": ['workspace']}},
        ]
        if additional_must:
            must_conditions.append(additional_must)

        if collection_id:
            must_conditions.append(
                {"bool" : {
                    "should": [
                        {"match": {"collections.path": collection_id }},
                        {"match": {"collections.nodeRef.id": collection_id}},
                    ],
                    "minimum_should_match": 1
                }
                }
            )
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
        Returns count of values for a given attribute (default: license) in a collection
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
            gt_timestamp = self.last_timestamp
            logging.info(f"searching with a gt timestamp of: {gt_timestamp}")
        else:
            gt_timestamp = timestamp
            logging.info(f"searching with a given timestamp of: {gt_timestamp}")

        body = {
            "query": { 
                "range": { 
                "timestamp": { 
                    "gt": gt_timestamp,
                    "lt": "now"
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

        # set last timestamp to last timestamp from response
        if len(r):
            self.last_timestamp = r[0].get("_source", {}).get("timestamp")

        filtered_search_strings = filter_search_strings(r)
        search_counter = Counter(list(filtered_search_strings))


        def filter_for_terms_and_materials(res: list[dict]):
            """
            :param list[dict] res: result from elastic-search query
            """
            terms_and_materials = defaultdict(list)
            materials_by_terms = defaultdict(SearchedMaterialInfo)
            filtered_res = (item for item in res if item.get("_source", {}).get("action", None) == "result_click")
            for item in (item.get("_source") for item in filtered_res):
                search_string: str = item.get("searchString")
                clicked_resource = item.get("clickedResult").get("id")
                terms_and_materials[search_string].append(clicked_resource)

                # we got to check the FPs for the given resource
                logging.info(f"checking included fps for resource id: {clicked_resource}")

                # build the object
                if not materials_by_terms.get(clicked_resource, None):
                    result: SearchedMaterialInfo = self.get_resource_info(clicked_resource, list(collections_ids_title.keys()))
                    materials_by_terms[clicked_resource].fps.update(result.fps)
                    materials_by_terms[clicked_resource].name = result.name
                    materials_by_terms[clicked_resource].title = result.title
                    materials_by_terms[clicked_resource]._id = clicked_resource
                    
                materials_by_terms[clicked_resource].search_strings.update([search_string])
                materials_by_terms[clicked_resource].clicks += 1
            return terms_and_materials, materials_by_terms


        # we have to check if path contains one of the edu-sharing collections with an elastic query
        # get fpm collections
        collections = EduSharing.get_collections()
        collections_ids_title = {item.get("properties").get("sys:node-uuid")[0]: item.get("title") for item in collections}
        terms_and_materials, materials_by_terms = filter_for_terms_and_materials(r)

        # assign material to fpm portals
        collections_by_material = defaultdict(list)
        for key in materials_by_terms: #key is the material id
            if fps:=materials_by_terms[key].fps:
                for fp in fps:
                    collections_by_material[fp].append(materials_by_terms[key])
            else:
                collections_by_material["none"].append(materials_by_terms[key])

        # check if searched_materials_by_collection does already exist.
        # in that case we only need to append new values to existing dictionary
        if self.searched_materials_by_collection:
            for key in collections_by_material:
                # TODO putting new material at the beginning of the list might be improved
                # it would be even better to look for the respective material and then append the search strings / update the Counter
                self.searched_materials_by_collection[key] = collections_by_material[key] + self.searched_materials_by_collection[key]
        else:
            self.searched_materials_by_collection = collections_by_material

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
            "_source": [
                "properties.cclom:title",
                "properties.cm:name",
                "collections.path",
                "properties.ccm:replicationsource"
                ]
        }
        return self.es.search(body=body, index="workspace", pretty=True)


    def get_resource_info(self, resource_id: str, collection_ids: list) -> SearchedMaterialInfo:
        """
        Gets info about a resource from elastic
        """
        try:
            hit: dict = self.get_node_path(resource_id).get("hits", {}).get("hits", [])[0]
            paths = hit.get("_source").get("collections", [{}])[0].get("path", [])
            name = hit.get("_source").get("properties", {}).get("cm:name", None) # internal name
            title = hit.get("_source").get("properties", {}).get("cclom:title", None) # readable title
            crawler = hit.get("_source").get("properties", {}).get("ccm:replicationsource", None)
            included_fps = [path for path in paths if path in collection_ids]
            return SearchedMaterialInfo(resource_id, name=name, title=title, crawler=crawler, fps=included_fps)
        except:
            return SearchedMaterialInfo()


    def get_aggregations(self, attribute: str, collection_id:str = None):
        """
        Returns the aggregations for a given attribute.
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(collection_id),
                    ]
                }
            },
            "size": 0, 
            "aggs": {
                "my-agg": {
                "terms": {
                    "field": attribute,
                    "size": 10000
                }
                }
            }
        }
        r: dict = self.es.search(body=body, index="workspace", pretty=True)
        
        def build_buckets(buckets):
            return [Bucket(b["key"], b["doc_count"]) for b in buckets]
        
        buckets: list[Bucket] = build_buckets(r.get("aggregations", {}).get("my-agg", {}).get("buckets", []))
        return buckets


if __name__ == "__main__":
    oeh = OEHElastic()
    print("\n\n\n\n")
    # print(json.dumps(oeh.getStatisicCounts("4940d5da-9b21-4ec0-8824-d16e0409e629"), indent=4))
    # print(json.dumps(oeh.get_material_by_condition("4940d5da-9b21-4ec0-8824-d16e0409e629", count=0), indent=4))
# ohne titel
    # print(json.dumps(oeh.getMaterialByMissingAttribute("4940d5da-9b21-4ec0-8824-d16e0409e629", "properties.ccm:commonlicense_key.keyword", 0), indent=4))
    # pprint(oeh.get_oeh_search_analytics())
    # oeh.get_oeh_search_analytics(count=200)
    # print("\n\n\n\n")
    # oeh.get_oeh_search_analytics(count=200)
    logging.info(oeh.get_aggregations(attribute="i18n.de_DE.ccm:educationallearningresourcetype.keyword"))
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
