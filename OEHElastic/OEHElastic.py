#!/usr/bin/env python3

import json
import logging
import os
from collections import Counter, defaultdict
from typing import Generator, Literal
from numpy import inf

import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

from time import sleep

from HelperClasses import SearchedMaterialInfo, Bucket

load_dotenv()

logging.basicConfig(level=logging.INFO)

ES_PREVIEW_URL = "https://redaktion.openeduhub.net/edu-sharing/preview?maxWidth=200&maxHeight=200&crop=true&storeProtocol=workspace&storeId=SpacesStore&nodeId={}"

SOURCE_FIELDS = [
    "nodeRef",
    "type",
    "preview",
    "properties.cclom:title",
    "properties.cm:name"
]


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

        logging.info(f"Collecting Collections from edu-sharing...")
        r_collections: list = requests.get(
            ES_COLLECTIONS_URL,
            headers=headers,
            params=params
        ).json().get("collections")

        return r_collections


class OEHElastic:

    es: Elasticsearch

    def __init__(self, hosts=[os.getenv("ES_HOST", "localhost")]) -> None:
        self.connection_retries = 0
        self.es = Elasticsearch(hosts=hosts)
        self.last_timestamp = "now-30d" # get values for last 30 days by default
        self.searched_materials_by_collection = {} # dict with collections as keys and a list of Searched Material Info as values
        self.all_searched_materials: set[SearchedMaterialInfo] = set() 

        self.get_oeh_search_analytics(timestamp = self.last_timestamp, count=1000)


    def query_elastic(self, body, index, pretty):
        try:
            self.connection_retries = 0
            r = self.es.search(body=body, index=index, pretty=pretty)
            return r
        except ConnectionError:
            if self.connection_retries < float(inf): # TODO put a number here?
                self.connection_retries += 1
                logging.error(f"Connection error, trying again in 30 seconds")
                sleep(30)
                return self.query_elastic(body, index, pretty)


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
        query = self.query_elastic(body=body, index="oeh-search-analytics", pretty=True)
        r: list[dict] = query.get("hits", {}).get("hits", [])

        # set last timestamp to last timestamp from response
        if len(r):
            self.last_timestamp = r[0].get("_source", {}).get("timestamp")

        filtered_search_strings = filter_search_strings(r)
        search_counter = Counter(list(filtered_search_strings))

        def check_timestamp(new, old):
            if new > old:
                return new
            else:
                return old


        def filter_for_terms_and_materials(res: list[dict]):
            """
            :param list[dict] res: result from elastic-search query
            """
            all_materials: set[SearchedMaterialInfo] = set()
            filtered_res = (item for item in res if item.get("_source", {}).get("action", None) == "result_click")
            for item in (item.get("_source", {}) for item in filtered_res):
                clicked_resource_id = item.get("clickedResult").get("id")
                timestamp: str = item.get("timestamp", "")
                
                clicked_resource = SearchedMaterialInfo(
                    _id=clicked_resource_id,
                    timestamp=timestamp
                    )
                
                search_string: str = item.get("searchString", "")

                # we got to check the FPs for the given resource
                logging.info(f"checking included fps for resource id: {clicked_resource}")

                # build the object
                if not clicked_resource in self.all_searched_materials:
                    logging.info(f"{clicked_resource} not present, creating entry, getting info...")
                    result: SearchedMaterialInfo = self.get_resource_info(clicked_resource._id, list(collections_ids_title.keys()))
                    result.timestamp = timestamp
                    result.search_strings.update([search_string])
                    self.all_searched_materials.add(result)
                else:
                    logging.info(f"{clicked_resource!r} present, updating...")
                    old = next(e for e in self.all_searched_materials if e == clicked_resource)
                    # check for newest timestamp
                    new_timestamp = check_timestamp(timestamp, old.timestamp)

                    clicked_resource.title = old.title
                    clicked_resource.name = old.name
                    clicked_resource.fps = old.fps
                    clicked_resource.creator = old.creator
                    clicked_resource.search_strings.update([search_string])
                    clicked_resource.search_strings += old.search_strings
                    clicked_resource.clicks = old.clicks + 1
                    clicked_resource.timestamp = new_timestamp
                    self.all_searched_materials.remove(old)
                    self.all_searched_materials.add(clicked_resource)

            return True


        # we have to check if path contains one of the edu-sharing collections with an elastic query
        # get fpm collections
        collections = EduSharing.get_collections()
        collections_ids_title = {item.get("properties").get("sys:node-uuid")[0]: item.get("title") for item in collections}
        materials_by_terms = filter_for_terms_and_materials(r)

        # assign material to fpm portals
        collections_by_material = defaultdict(list)
        for item in sorted(self.all_searched_materials, reverse=True): #key is the material id
            if fps:=item.fps:
                for fp in fps:
                    collections_by_material[fp].append(item)
            else:
                collections_by_material["none"].append(item)

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
                "properties.ccm:replicationsource",
                "properties.cm:creator"
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
            crawler = hit.get("_source").get("properties", {}).get(
                "ccm:replicationsource", None)
            creator = hit.get("_source").get("properties", {}).get(
                "cm:creator", None)
            included_fps = [path for path in paths if path in collection_ids]
            return SearchedMaterialInfo(
                resource_id,
                name=name,
                title=title,
                clicks=1,
                crawler=crawler,
                creator=creator,
                fps=included_fps
                )
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


    def sort_searched_materials(self) -> list[SearchedMaterialInfo]:
        """
        Sorts searched materials by last click.
        """
        searched_materials_all: set[SearchedMaterialInfo] = set()
        for key in self.searched_materials_by_collection:
            searched_materials_all.update(self.searched_materials_by_collection[key])
        sorted_search = sorted(
            searched_materials_all,
            key=lambda x: x.timestamp,
            reverse=True)
        return sorted_search


    def get_material_creators(self):
        # make a general aggregation query method and combine with crawler method
        # by aggregation query?
        # GET workspace/_search
        # {
        #     "size": 0,
        #     "aggs": {
        #         "my-agg": {
        #             "terms": {
        #                 "field": "properties.cm:creator.keyword",
        #                 "size": 10000
        #             }
        #         }
        #     }
        # }
        pass


if __name__ == "__main__":
    oeh = OEHElastic()
    print("\n\n\n\n")

    oeh.get_oeh_search_analytics(count=200)
    oeh.get_oeh_search_analytics(count=200)
    oeh.sort_searched_materials()

else:
    oeh = OEHElastic()
