#%%
from elasticsearch import Elasticsearch
import requests
from dataclasses import dataclass, field
from OEHElastic import OEHElastic
from collections import namedtuple
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import logging
from typing import Literal
from Constants import fpm_icons


logging.basicConfig(level=logging.INFO)

# CONSTANTS
ES_QUERY_URL = "https://redaktion.openeduhub.net/edu-sharing/components/search?query="

oeh = OEHElastic()


@dataclass
class MissingInfo:
    _id: str
    name: str = ""
    title: str = ""
    es_url: str = field(init=False)

    def __post_init__(self):
        self.es_url = ES_QUERY_URL + self._id


class Collection:
    """
    Container class for a Fachportal-Collection, i.e. the whole Physik or Mathematik Fachportal.
    It is NOT a pendant to an edu-sharing collection!
    """
    def __init__(self, item: dict):
        self.name: str = item.get("name", None)
        self.title: str = item.get("title", None)
        self.iconURL: str = item.get("iconURL", "") # icon of edu-sharing collection
        self.url: str = item.get("content").get("url") # edu-sharing url of the collection
        self.app_url: str = self.make_url()
        self._id: str = item.get("properties").get("sys:node-uuid")[0]
        self.about: str = item.get("properties", {}).get("ccm:taxonid", [""])[0]
        self.resources_total: int = 0
        #TODO i guess self_id does not have to be passed as an argument
        self.licenses: dict = {}
        self.resources_no_title_identifiers: list[MissingInfo] = []
        self.resources_no_subject_identifiers: list[MissingInfo] = []
        self.resources_no_educontext: list[MissingInfo] = []
        self.resources_no_keywords: list[MissingInfo] = []
        self.resources_no_licenses: list[MissingInfo] = []
        self.collection_no_keywords: list[MissingInfo] = []
        self.collection_no_description: list[MissingInfo] = []
        self.quality_score: int = 0
        self.layout = html.Div()


    def __lt__(self, other):
        return self.name < other.name


    def __repr__(self):
        return self.name


    def load_data(self):
        self.resources_total: int = self.get_resources_total()
        self.resources_no_licenses: list[MissingInfo] = self.get_missing_attribute(None, qtype="license")
        self.licenses: dict = self.get_licenses()
        self.resources_no_title_identifiers: list[MissingInfo] = self.get_missing_attribute("properties.cclom:title", qtype="resource")
        self.resources_no_subject_identifiers: list[MissingInfo] = self.get_missing_attribute("properties.ccm:taxonid", qtype="resource")
        self.resources_no_educontext: list[MissingInfo] = self.get_missing_attribute("properties.ccm:educationalcontext", qtype="resource")
        self.resources_no_keywords: list[MissingInfo] = self.get_missing_attribute("properties.cclom:general_keyword", qtype="resource")
        self.collection_no_keywords: list[MissingInfo] = self.get_missing_attribute("properties.cclom:general_keyword", qtype="collection")
        self.collection_no_description: list[MissingInfo] = self.get_missing_attribute("properties.cm:description", qtype="collection")
        self.quality_score = self.calc_quality_score()
        self.layout = self.build_layout()


    def calc_quality_score(self):
        # TODO add licenses
        score_items = [
            self.resources_no_title_identifiers,
            self.resources_no_subject_identifiers,
            self.resources_no_educontext,
            self.resources_no_keywords,
            self.collection_no_keywords,
            self.collection_no_description
        ]
        score = 0

        for item in score_items:
            try:
                score += ((1 - (len(item) / self.resources_total)) / len(score_items))
            except ZeroDivisionError:
                logging.error(f"Zero Division Error with Collection: {self.name}")
                return 0

        return round(score, 2) * 100


    def sort_licenses(self, licenses):
        oer_cols = ["CC_0", "CC_BY", "CC_BY_SA", "PDM"]
        cc_but_not_oer = ["CC_BY_NC", "CC_BY_NC_ND",
                      "CC_BY_NC_SA", "CC_BY_SA_NC", "CC_BY_ND"]
        copyright_cols = ["COPYRIGHT_FREE",	"COPYRIGHT_LICENSE", "CUSTOM"]
        missing_cols = ["", "NONE", "UNTERRICHTS_UND_LEHRMEDIEN"]

        licenses_sorted = {
            "OER-Lizenz": 0,
            "CC-Lizenz": 0,
            "Copyright-Lizenz": 0,
            "Fehlende Lizenz": 0
        }

        for l in licenses:
            if l["key"] in oer_cols:
                licenses_sorted["OER-Lizenz"] += l["doc_count"]
            elif l["key"] in cc_but_not_oer:
                licenses_sorted["CC-Lizenz"] += l["doc_count"]
            elif l["key"] in copyright_cols:
                licenses_sorted["Copyright-Lizenz"] += l["doc_count"]
            elif l["key"] in missing_cols:
                licenses_sorted["Fehlende Lizenz"] += l["doc_count"]
            else:
                raise KeyError(f'Could not find {l["key"]} in columns.')

        # some licenses are not counted here, because the property "properties.ccm:commonlicense_key.keyword"
        # does not exist on these resources. We have to add them by a query to count missing attributes
        licenses_sorted["Fehlende Lizenz"] = len(self.resources_no_licenses)

        return licenses_sorted


    def get_licenses(self):
        r: list[dict] = oeh.getStatisicCounts(self._id, "properties.ccm:commonlicense_key.keyword").get("aggregations", {}).get("license", {}).get("buckets", [])
        licenses = self.sort_licenses(r)
        return licenses


    def get_resources_total(self):
        r: int = oeh.getStatisicCounts(self._id).get("hits", {}).get("total", {}).get("value", 0)
        return r

    # TODO add a flag
    # if resource return render url
    # if collection return query url
    def build_link_container(self, list_of_values: list[MissingInfo]):
        container = []
        for i in list_of_values:
            container.append(
                html.Div(
                    children=[
                        html.P(
                            children=[
                                html.A(
                                    children=f"{i.title if i.title else i.name}",
                                    href=f"{i.es_url}",
                                    target="_blank"
                                    )
                            ]
                        )
                    ]
                )
            )
        return container


    def build_license_fig(self):
        """
        Builds a licenses Dataframe with columns: OER, CC-Lizenz, Copyright-Lizenz and Fehlende Lizenz.
        """
        labels = list(self.licenses.keys())
        sizes = list(self.licenses.values())
        pull = (0.1, 0, 0, 0)

        fig = go.Figure(data=[go.Pie(labels=labels, values=sizes, pull=pull)])

        # make background transparent
        fig.update_layout({
            'paper_bgcolor': 'rgba(0,0,0,0)',
            'plot_bgcolor': 'rgba(0,0,0,0)'
        })

        return fig


    def build_missing_info_card(self, title: str, attribute: list):
        """
        Returns a div with the infos for missing resources.
        """
        return html.Div(
            children=[
                html.P(
                    f"{title} ({len(attribute)}):"),
                html.Div(
                    children=self.build_link_container(attribute),
                    className="card"
                )
            ],
            className="card-box"
        )


    def build_layout(self):
        res_no_title = self.build_missing_info_card("Materialien ohne Titel", self.resources_no_title_identifiers)
        res_no_subject = self.build_missing_info_card("Materialien ohne Fachzuordnung", self.resources_no_subject_identifiers)
        res_no_educontext = self.build_missing_info_card("Materialien ohne Zuordnung der Bildungstufe", self.resources_no_educontext)
        res_no_keywords = self.build_missing_info_card("Materialien ohne Schlagworte", self.resources_no_keywords)
        res_no_license = self.build_missing_info_card("Materialien ohne Lizenz", self.resources_no_licenses)
        coll_no_keywords = self.build_missing_info_card("Sammlungen ohne Schlagworte", self.collection_no_keywords)
        coll_no_description = self.build_missing_info_card("Sammlung ohne Beschreibungstext", self.collection_no_description)
        return html.Div(
            children=[
                html.Div(
                    children=[
                        html.A(
                            "Zurück",
                            className="back-btn",
                            href="/"),
                        html.P(children="🔎", className="header-emoji"),
                        html.H1(
                            children=f"WLO Analytics ({self.name})", className="header-title"
                        ),
                        html.P(
                            children="Hilf uns die WLO-Suche noch besser zu machen!",
                            className="header-description",
                        ),
                    ],
                    className="header",
                ),
                html.Div(
                    className="info-row-0",
                    children=[
                        html.Div(
                            className="card-box",
                            children=[
                                html.H3("Materialien in deinem Fachportal"),
                                html.P(self.resources_total,
                                       className="sum-material"),
                                html.H3("Datenqualitätsscore"),
                                html.P(self.quality_score,
                                    className="quality-score",
                                    **{"data-status": f"{self.quality_score}"},
                                )
                            ]
                        ),
                        html.Div(
                            className="card-box",
                            children=[
                                html.H3("Lizenzen in Deinem Portal"),
                                html.Div(
                                    className="card",
                                    children=[
                                        dcc.Graph(id="pie-chart", figure=self.build_license_fig()), ]
                                )
                            ]
                        )
                    ]
                ),
                html.H2(
                    "Die Materialien in deinem Fachportal",
                    className="row-header"),
                html.Div(
                    className="info-row-1",
                    children=[
                        res_no_title,
                        # TODO
                        # html.Div(
                        #     children=[
                        #         html.P(
                        #             f"Materialien ohne Lizenz ({len(fpm_data.no_license_container)}):"),
                        #         html.Div(
                        #             children=fpm_data.no_license_container,
                        #             className="card"
                        #         )
                        #     ],
                        #     className="card-box"
                        # ),
                        res_no_license,
                        res_no_subject,
                        res_no_educontext,
                        res_no_keywords
                    ]
                ),
# end materialien
                html.H2(
                    "Deine Sammlungen",
                    className="row-header"
                ),

                html.Div(
                    className="info-row-1",
                    children=[
                        coll_no_description,
                        coll_no_keywords,
                        
                        # html.Div(
                        #     children=[
                        #         html.P(
                        #             f"Sammlungen ohne Materialien ({len(fpm_data.no_resources_in_collection_container)}):"),
                        #         html.Div(
                        #             children=fpm_data.no_resources_in_collection_container,
                        #             className="card"
                        #         )
                        #     ],
                        #     className="card-box"
                        # ),

                    ]
                )

            ]
        )


    def get_missing_attribute(self, attribute, qtype: Literal["collection", "resource", "license"]):
        """
        Gets the missing attributes 
        """
        if qtype == "resource":
            r: list = oeh.getMaterialByMissingAttribute(self._id, attribute).get("hits", {}).get("hits", [])
        elif qtype == "collection":
            r: list = oeh.getCollectionByMissingAttribute(self._id, attribute).get("hits", {}).get("hits", [])
        elif qtype == "license":
            # some resources don't have a license keyword others have one, but it is NONE, "" or something strange
            # so we need to combine this here 
            r1: list = oeh.get_material_by_condition(self._id, condition="missing_license").get("hits", {}).get("hits", [])
            r2: list = oeh.getMaterialByMissingAttribute(self._id, attribute="properties.ccm:commonlicense_key.keyword").get("hits", {}).get("hits", [])
            r: list = r1 + r2
        else:
            raise ValueError("qtype is not of: collection, resource, license")
        result: list[MissingInfo] = [self.parse_result(item) for item in r]
        return result

    def parse_result(self, resource):
        _id = resource.get("_source", {}).get("nodeRef", {}).get("id", None)
        name = resource.get("_source", {}).get("properties", {}).get("cm:name", None)
        title = resource.get("_source", {}).get("properties", {}).get("cclom:title", None)
        return MissingInfo(_id, name, title)

    def make_url(self):
        return self.name.lower().replace(" ", "-").replace("ü", "ue")


class Collections:
    def __init__(self):
        self.collections: list[Collection] = self.get_collections()
        self.cards_for_index_page = self.build_cards_for_index_page() #cards for index page
        self.pathnames = self.build_pathnames() # the pathnames e.g. "/physik"

    def build_pathnames(self):
        return ["/" + item.app_url for item in self.collections]

    def get_collections(self):
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

        collections = sorted([Collection(item) for item in r_collections])

        return collections


    def build_cards_for_index_page(self) -> list:
            """
            build index links
            """
            index_links = []
            for item in self.collections:
                index_links.extend([
                    dcc.Link(
                        href=item.app_url,
                        className="fpm-card",
                        children=[
                            html.Img(src=fpm_icons.get(item.name, item.iconURL)),
                            html.P(f"{item.title}")
                        ]
                    )
                ])
            return index_links

if __name__ == "__main__":
    c = Collections()
    logging.info(c.collections)
    c.collections[0].load_data()

# %%
