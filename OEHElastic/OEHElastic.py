#!/usr/bin/env python3

import logging
import os
from collections import Counter, defaultdict
from time import sleep
from typing import Generator, Literal

import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from HelperClasses import Bucket, MissingInfo, SearchedMaterialInfo
from numpy import inf

load_dotenv()

logger = logging.getLogger(__name__)


def set_conn_retries():
    MAX_CONN_RETRIES = os.getenv("MAX_CONN_RETRIES", float(inf))
    if MAX_CONN_RETRIES == "inf":
        return float(inf)
    else:
        if type(eval(MAX_CONN_RETRIES)) == int:
            return eval(MAX_CONN_RETRIES)
        else:
            raise TypeError(
                f"MAX_CONN_RETRIES: {eval(MAX_CONN_RETRIES)} is not an integer")


MAX_CONN_RETRIES = set_conn_retries()
ES_PREVIEW_URL = "https://redaktion.openeduhub.net/edu-sharing/preview?maxWidth=200&maxHeight=200&crop=true&storeProtocol=workspace&storeId=SpacesStore&nodeId={}"
SOURCE_FIELDS = [
    "nodeRef",
    "type",
    "preview",
    "properties.cclom:title",
    "properties.cm:name"
]
ANALYTICS_INITIAL_COUNT = eval(os.getenv("ANALYTICS_INITIAL_COUNT", 10000))


class EduSharing:
    connection_retries: int = 0

    @classmethod
    def get_collections(cls):
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

        logger.info(f"Collecting Collections from edu-sharing...")

        try:
            r_collections: list = requests.get(
                ES_COLLECTIONS_URL,
                headers=headers,
                params=params
            ).json().get("collections")
            cls.connection_retries = 0
            return r_collections

        except:
            if cls.connection_retries < MAX_CONN_RETRIES:
                cls.connection_retries += 1
                logger.error(
                    f"Connection error trying to reach edu-sharing repository, trying again in 30 seconds. Retries: {cls.connection_retries}")
                sleep(30)
                return EduSharing.get_collections()


class OEHElastic:
    es: Elasticsearch

    def __init__(self, hosts=[os.getenv("ES_HOST", "localhost")]) -> None:
        self.connection_retries = 0
        self.es = Elasticsearch(hosts=hosts)
        self.last_timestamp = "now-30d"  # get values for last 30 days by default
        # dict with collections as keys and a list of Searched Material Info as values
        self.searched_materials_by_collection: dict[str, SearchedMaterialInfo] = {}
        self.all_searched_materials: set[SearchedMaterialInfo] = set()

        self.get_oeh_search_analytics(
            timestamp=self.last_timestamp, count=ANALYTICS_INITIAL_COUNT)

    def collections_by_fachportale(
        self,
        fachportal_key: str = None,
        doc_threshold: int = 0,
        collection_ids: list = []
        ):
        """
        Returns a dict of Fachportal-IDs as keys and a list of collection ids as values
        if there is no material present in that collection.

        :param fachportal_key: ID of the Fachportal
        """
        logger.info(f"getting collections with threshold of {doc_threshold} and key: {fachportal_key}")
        def check_for_resources_in_subcollection(collection_id: str):
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "type": [
                                        "ccm:io"
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "should": [
                                        {
                                            "match": {
                                                "path": collection_id
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                "_source": ""
            }
            r = self.query_elastic(body=body, index="workspace")
            total_hits = r.get("hits").get("total").get("value")

            if total_hits <= doc_threshold:
                return True
            else:
                return False

        def build_missing_info(r: list[dict]) -> MissingInfo:
            buckets = self.get_aggregations(
                attribute="collections.nodeRef.id.keyword")
            res = set()
            for item in r:
                _id = item.get("_source").get("nodeRef").get("id")
                title = item.get("_source").get("properties").get("cm:title")

                # check if a corresponding collection is in buckets and add doc count from there
                doc_count = next((bucket.doc_count for bucket in buckets if bucket == _id), 0)
                if doc_count <= doc_threshold:
                    # check if there is content in subcollections
                    if check_for_resources_in_subcollection(_id):
                        res.add(MissingInfo(_id=_id, title=title, doc_count=doc_count, _type="ccm:map"))
            return res

        if fachportal_key:
            r_collection_children = self.get_collection_children_by_id(fachportal_key)
            collection_children: set[MissingInfo] = build_missing_info(
                r_collection_children.get("hits", {}).get("hits", []))
            return collection_children
        else:
            present_collections: dict[str, set[MissingInfo]] = {}

            for key in collection_ids:
                # for each fp portal query es for all of its collections
                r_collection_children = self.get_collection_children_by_id(key)
                collection_children: set[MissingInfo] = build_missing_info(r_collection_children.get("hits", {}).get("hits", []))
                present_collections[key] = collection_children
            return dict(sorted(present_collections.items()))


    def query_elastic(self, body, index, pretty: bool = True):
        try:
            r = self.es.search(body=body, index=index, pretty=pretty)
            self.connection_retries = 0
            return r
        except ConnectionError:
            if self.connection_retries < MAX_CONN_RETRIES:
                self.connection_retries += 1
                logger.error(
                    f"Connection error while trying to reach elastic instance, trying again in 30 seconds. Retries {self.connection_retries}")
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
                {"bool": {
                    "should": [
                        {"match": {"collections.path": collection_id}},
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

    def getCollectionByMissingAttribute(self, collection_id: str, attribute: str, count: int = 10000) -> dict:
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
                        {"bool": {
                            "should": [
                                {"match": {"path": collection_id}},
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
        return self.query_elastic(body=body, index="workspace", pretty=True)


    def get_collection_children_by_id(self, collection_id: str):
        """
        Returns a list of children of a given collection_id
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"type": ["ccm:map"]}},
                        {"bool": {
                            "should": [
                                {"match": {"path": collection_id}},
                                ]   
                            }
                        }
                    ]
                }
            },
            "size": 10000,
            "track_total_hits": "true",
            "_source": ["properties.cm:title", "nodeRef.id"]
        }
        return self.query_elastic(body=body, index="workspace", pretty=True)


    def getMaterialByMissingAttribute(self, collection_id: str, attribute: str, count: int = 10000) -> dict:
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
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def getStatisicCounts(self, collection_id: str, attribute: str = "properties.ccm:commonlicense_key.keyword") -> dict:
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
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_material_by_condition(self, collection_id: str, condition: Literal["missing_license"] = None, count=10000) -> dict:
        """
        Returns count of values for a given attribute (default: license)
        """
        if condition == "missing_license":
            additional_condition = {
                "terms": {
                    "properties.ccm:commonlicense_key.keyword": ["NONE", "", "UNTERRICHTS_UND_LEHRMEDIEN"]
                }
            }
        else:
            additional_condition = None
        body = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(
                            collection_id, additional_condition),
                    ]
                }
            },
            "_source": SOURCE_FIELDS,
            "size": count,
            "track_total_hits": True
        }
        # print(body)
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_oeh_search_analytics(self, timestamp: str = None, count: int = 10000):
        """
        Returns the oeh search analytics.
        """
        def filter_search_strings(unfiltered: list[dict]) -> Generator:
            for item in unfiltered:
                search_string = item.get(
                    "_source", {}).get("searchString", None)
                if search_string and search_string.strip() != "":
                    yield search_string
                else:
                    continue

        if not timestamp:
            gt_timestamp = self.last_timestamp
            logger.info(f"searching with a gt timestamp of: {gt_timestamp}")
        else:
            gt_timestamp = timestamp
            logger.info(f"searching with a given timestamp of: {gt_timestamp}")

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
        query = self.query_elastic(
            body=body, index="oeh-search-analytics", pretty=True)
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
            filtered_res = (item for item in res if item.get(
                "_source", {}).get("action", None) == "result_click")
            for item in (item.get("_source", {}) for item in filtered_res):
                clicked_resource_id = item.get("clickedResult").get("id")
                timestamp: str = item.get("timestamp", "")

                clicked_resource = SearchedMaterialInfo(
                    _id=clicked_resource_id,
                    timestamp=timestamp
                )

                search_string: str = item.get("searchString", "")

                # we got to check the FPs for the given resource
                logger.info(
                    f"checking included fps for resource id: {clicked_resource}")

                # build the object
                if not clicked_resource in self.all_searched_materials:
                    logger.info(
                        f"{clicked_resource} not present, creating entry, getting info...")
                    result: SearchedMaterialInfo = self.get_resource_info(
                        clicked_resource._id, list(collections_ids_title.keys()))
                    result.timestamp = timestamp
                    result.search_strings.update([search_string])
                    self.all_searched_materials.add(result)
                else:
                    logger.info(f"{clicked_resource!r} present, updating...")
                    old = next(
                        e for e in self.all_searched_materials if e == clicked_resource)
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
        collections_ids_title = {item.get("properties").get(
            "sys:node-uuid")[0]: item.get("title") for item in collections}
        materials_by_terms = filter_for_terms_and_materials(r)

        # assign material to fpm portals
        collections_by_material = defaultdict(list)
        for item in sorted(self.all_searched_materials, reverse=True):  # key is the material id
            if fps := item.fps:
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
        return self.query_elastic(body=body, index="workspace", pretty=True)

    def get_resource_info(self, resource_id: str, collection_ids: list) -> SearchedMaterialInfo:
        """
        Gets info about a resource from elastic
        """
        try:
            hit: dict = self.get_node_path(resource_id).get(
                "hits", {}).get("hits", [])[0]
            paths = hit.get("_source").get(
                "collections", [{}])[0].get("path", [])
            name = hit.get("_source").get("properties", {}).get(
                "cm:name", None)  # internal name
            title = hit.get("_source").get("properties", {}).get(
                "cclom:title", None)  # readable title
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

    def get_aggregations(self, attribute: str, collection_id: str = None, index: str = "workspace", size: int = 10000):
        """
        Returns the aggregations for a given attribute.
        Return is a list of dicts with keys: key, doc_count
        """
        must_condition = {
            "query": {
                "bool": {
                    "must": [
                        self.getBaseCondition(collection_id),
                    ]
                }
            }
        }
        body = {
            "size": 0,
            "aggs": {
                "my-agg": {
                    "terms": {
                        "field": attribute,
                        "size": size
                    }
                }
            }
        }
        if index == "workspace":
            body.update(must_condition)
        r: dict = self.es.search(body=body, index=index, pretty=True)

        def build_buckets(buckets):
            return [Bucket(b["key"], b["doc_count"]) for b in buckets]

        buckets: list[Bucket] = build_buckets(
            r.get("aggregations", {}).get("my-agg", {}).get("buckets", []))
        return buckets

    def sort_searched_materials(self) -> list[SearchedMaterialInfo]:
        """
        Sorts searched materials by last click.
        """
        searched_materials_all: set[SearchedMaterialInfo] = set()
        for key in self.searched_materials_by_collection:
            searched_materials_all.update(
                self.searched_materials_by_collection[key])
        sorted_search = sorted(
            searched_materials_all,
            key=lambda x: x.timestamp,
            reverse=True)
        return sorted_search


if __name__ == "__main__":
    oeh = OEHElastic()
    print("\n\n\n\n")

    oeh.collections_by_fachportale

else:
    oeh = OEHElastic()
