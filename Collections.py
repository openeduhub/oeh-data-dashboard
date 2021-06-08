import logging
from dataclasses import dataclass, field
from typing import Literal, Type, TypedDict

import dash_core_components as dcc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
import pandas as pd

from Constants import fpm_icons
from OEHElastic import EduSharing, OEHElastic, SearchedMaterialInfo

logging.basicConfig(level=logging.INFO)

# CONSTANTS
ES_COLLECTION_URL = "https://redaktion.openeduhub.net/edu-sharing/components/collections?id={}"
ES_NODE_URL = "https://redaktion.openeduhub.net/edu-sharing/components/render/{}?action={}"
ES_PREVIEW_URL = "https://redaktion.openeduhub.net/edu-sharing/preview?maxWidth=200&maxHeight=200&crop=true&storeProtocol=workspace&storeId=SpacesStore&nodeId={}"

oeh = OEHElastic()


class Licenses(TypedDict):
    oer: int
    cc: int
    copyright: int
    missing: int

@dataclass
class MissingInfo:
    _id: str
    name: str = ""
    title: str = ""
    _type: str = ""
    action: str = ""
    es_url: str = field(init=False)

    def __post_init__(self):
        if self._type == 'ccm:map':
            self.es_url = ES_COLLECTION_URL.format(self._id)
        else:
            self.es_url = ES_NODE_URL.format(self._id, self.action)


class Collection:
    """
    Container class for a Fachportal-Collection, i.e. the whole Physik or Mathematik Fachportal.
    It is NOT a pendant to an edu-sharing collection!
    """
    def __init__(self, item: dict):
        self.name: str = item.get("name", None) # internal name
        self.title: str = item.get("title", None) # readable title
        self.iconURL: str = item.get("iconURL", "") # icon of edu-sharing collection
        self.url: str = item.get("content").get("url") # edu-sharing url of the collection
        self.app_url: str = self.make_url()
        self._id: str = item.get("properties").get("sys:node-uuid")[0]
        self.about: str = item.get("properties", {}).get("ccm:taxonid", [""])[0]

        self.clicked_materials: list[SearchedMaterialInfo] = oeh.searched_materials_by_collection.get(self._id, [])

        self._resources_total: int = 0
        self.licenses: Licenses = {}
        self._resources_no_title_identifiers: list[MissingInfo] = []
        self.resources_no_subject_identifiers: list[MissingInfo] = []
        self.resources_no_educontext: list[MissingInfo] = []
        self._resources_no_keywords: list[MissingInfo] = []
        self.resources_no_licenses: list[MissingInfo] = []
        self.collection_no_keywords: list[MissingInfo] = []
        self.collection_no_description: list[MissingInfo] = []
        self.quality_score: int = 0


    def __lt__(self, other):
        return self.name < other.name


    def __repr__(self):
        return self.name


    def as_dict(self):
        return {
            "name": self.name,
            "resources_total": self.resources_total,
            "resources_no_title_identifiers": len(self.resources_no_title_identifiers),
            "resources_no_keywords": len(self.resources_no_keywords),
            "oer_licenes": self.licenses.get("oer")
        }


    @property
    def resources_total(self):
        return self._resources_total

    @resources_total.getter
    def resources_total(self):
        return self.get_resources_total()

    @property
    def resources_no_title_identifiers(self):
        return self._resources_no_title_identifiers

    @resources_no_title_identifiers.getter
    def resources_no_title_identifiers(self):
        return self.get_missing_attribute("properties.cclom:title", qtype="resource")

    @property
    def resources_no_keywords(self):
        return self._resources_no_keywords

    @resources_no_keywords.getter
    def resources_no_keywords(self):
        return self.get_missing_attribute("properties.cclom:general_keyword", qtype="resource")

    @property
    def layout(self):
        return self._layout

    @layout.getter
    def layout(self):
        logging.info("Setting layout...")
        self.clicked_materials: list[SearchedMaterialInfo] = oeh.searched_materials_by_collection.get(self._id, [])
        self.resources_no_licenses: list[MissingInfo] = self.get_missing_attribute(None, qtype="license")
        self.licenses: dict = self.get_licenses()
        self.resources_no_subject_identifiers: list[MissingInfo] = self.get_missing_attribute("properties.ccm:taxonid", qtype="resource")
        self.resources_no_educontext: list[MissingInfo] = self.get_missing_attribute("properties.ccm:educationalcontext", qtype="resource")
        self.collection_no_keywords: list[MissingInfo] = self.get_missing_attribute("properties.cclom:general_keyword", qtype="collection")
        self.collection_no_description: list[MissingInfo] = self.get_missing_attribute("properties.cm:description", qtype="collection")
        self.quality_score = self.calc_quality_score()
        return self.build_layout()


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

        licenses_sorted: Licenses = {
            "oer": 0,
            "cc": 0,
            "copyright": 0,
            "missing": 0
        }

        for l in licenses:
            if l["key"] in oer_cols:
                licenses_sorted["oer"] += l["doc_count"]
            elif l["key"] in cc_but_not_oer:
                licenses_sorted["cc"] += l["doc_count"]
            elif l["key"] in copyright_cols:
                licenses_sorted["copyright"] += l["doc_count"]
            elif l["key"] in missing_cols:
                licenses_sorted["missing"] += l["doc_count"]
            else:
                raise KeyError(f'Could not find {l["key"]} in columns.')

        # some licenses are not counted here, because the property "properties.ccm:commonlicense_key.keyword"
        # does not exist on these resources. We have to add them by a query to count missing attributes
        licenses_sorted["missing"] = len(self.resources_no_licenses)

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
                                    children=[
                                        html.Div(
                                            children=[
                                                html.Span(f"{i.title if i.title else i.name}"),
                                                html.Img(src=ES_PREVIEW_URL.format(i._id)),
                                            ]
                                        )
                                    ],
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
        labels = ["OER", "CC-Lizenz", "Copyright-Lizenz", "Fehlende Lizenz"]
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

    @classmethod
    def build_searched_materials(cls, title, materials: list[SearchedMaterialInfo] = []):
        # df = pd.DataFrame([x.as_dict() for x in materials])
        # layout = dash_table.DataTable(
        #     id="table",
        #     columns=[{"name": i, "id": i} for i in df.columns],
        #     data=df.to_dict('records')
        # )
        # return layout
        clicked_materials = []
        search_term_count = "Suchbegriff: \"{}\" ({})" # term, count
        for material in materials:
            search_term_comprehension = " ".join([search_term_count.format(term, count) for term, count in material.search_strings.items()])
            clicked_materials.append(
                html.P(
                    children=[
                        html.A(
                            children=[
                                html.Div(
                                    children=[
                                        html.Span(f"{material.title if material.title else material.name}"),
                                        html.Span(f"{search_term_comprehension}, Klicks auf Material: {material.clicks}", style={
                                            "text-align": "right",
                                            "padding-right": "10px"
                                            }),
                                        html.Img(src=ES_PREVIEW_URL.format(material._id))
                                    ]
                                )
                            ],
                            href=f"{ES_NODE_URL.format(material._id, None)}",
                            target="_blank"
                        )
                    ]
                )
            )
                # html.P(f"{material._id}: {search_term_comprehension}, {material.clicks}")
        return html.Div(
            className="card-box",
            children=[
                html.P(f"{title}"),
                html.Div(
                    children=[*clicked_materials],
                    className="card"
                    ) 
                ]
            )


    def build_layout(self):
        res_no_title = self.build_missing_info_card("Materialien ohne Titel", self.resources_no_title_identifiers)
        res_no_subject = self.build_missing_info_card("Materialien ohne Fachzuordnung", self.resources_no_subject_identifiers)
        res_no_educontext = self.build_missing_info_card("Materialien ohne Zuordnung der Bildungstufe", self.resources_no_educontext)
        res_no_keywords = self.build_missing_info_card("Materialien ohne Schlagworte", self.resources_no_keywords)
        res_no_license = self.build_missing_info_card("Materialien ohne Lizenz", self.resources_no_licenses)
        coll_no_keywords = self.build_missing_info_card("Sammlungen ohne Schlagworte", self.collection_no_keywords)
        coll_no_description = self.build_missing_info_card("Sammlung ohne Beschreibungstext", self.collection_no_description)
        searched_materials = self.build_searched_materials("Diese Materialien aus deinem Fachportal wurden gesucht und geklickt (~letze 30 Tage)", self.clicked_materials)
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

                        # TODO
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
                ),
                html.Div(
                    className="info-row-2",
                    children=[
                        searched_materials
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
        result: list[MissingInfo] = [self.parse_result(item, qtype) for item in r]
        return result

    def parse_result(self, resource: dict, qtype: Literal["collection", "resource", "license"]):
        _id = resource.get("_source", {}).get("nodeRef", {}).get("id", None)
        name = resource.get("_source", {}).get("properties", {}).get("cm:name", None)
        title = resource.get("_source", {}).get("properties", {}).get("cclom:title", None)
        _type = resource.get("_source", {}).get("type", None)
        # action hint for edu-sharing to open dialog
        action = "OPTIONS.EDIT"
        if qtype == "license":
            action = "OPTIONS.LICENSE"
        return MissingInfo(_id, name, title, _type, action)

    def make_url(self):
        return self.name.lower().replace(" ", "-").replace("ü", "ue")


class Collections:
    def __init__(self):
        self.collections: list[Collection] = self.get_collections()
        self.cards_for_index_page = self.build_cards_for_index_page() #cards for index page
        self.pathnames: list[str] = self.build_pathnames() # the pathnames e.g. "/physik"
        self.searched_materials_not_in_collections = oeh.searched_materials_by_collection.get("none")
        self.searched_materials_not_in_collections_layout = Collection.build_searched_materials("Geklickte Materialien, die in keinem Fachportal liegen", self.searched_materials_not_in_collections) #searched_materials
        self._admin_page_layout = None
    
    def get_oeh_search_analytics(self):
        oeh.get_oeh_search_analytics()
        self.searched_materials_not_in_collections = oeh.searched_materials_by_collection.get("none")
        self.searched_materials_not_in_collections_layout = Collection.build_searched_materials("Geklickte Materialien, die in keinem Fachportal liegen", self.searched_materials_not_in_collections) #searched_materials

    def build_pathnames(self):
        return ["/" + item.app_url for item in self.collections]

    def get_collections(self):
        collections = sorted([Collection(item) for item in EduSharing.get_collections()])
        return collections

    def build_index_page(self):
        index_page = html.Div(
            children=[
                html.Div(className="index-container",
                        children=[
                            *self.cards_for_index_page,
                        ]),
                html.Div(
                    className="info-row-2",
                    children=[
                        self.searched_materials_not_in_collections_layout
                        ]
                    )
                ]
        )
        return index_page


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


    @property
    def admin_page_layout(self):
        return self._admin_page_layout

    @admin_page_layout.getter
    def admin_page_layout(self):
        # build dataframe
        d = [c.as_dict() for c in self.collections]
        df = pd.DataFrame(d)
        return dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records'),
        )


if __name__ == "__main__":
    c = Collections()
    logging.info(c.collections)
    c.collections[0].layout

# %%
