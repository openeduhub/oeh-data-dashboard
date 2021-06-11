from OEHElastic.OEHElastic import oeh
import dash_html_components as html

from HelperClasses import SearchedMaterialInfo, Licenses, MissingInfo

class Collection:
    """
    Container class for a Fachportal-Collection, i.e. the whole Physik or Mathematik Fachportal.
    It is NOT a pendant to an edu-sharing collection!
    """

    def __init__(self, item: dict):
        self.name: str = item.get("name", None)  # internal name
        self.title: str = item.get("title", None)  # readable title
        # icon of edu-sharing collection
        self.iconURL: str = item.get("iconURL", "")
        self.url: str = item.get("content").get(
            "url")  # edu-sharing url of the collection
        self.app_url: str = self.make_url()
        self._id: str = item.get("properties").get("sys:node-uuid")[0]
        self.about: str = item.get(
            "properties", {}).get("ccm:taxonid", [""])[0]

        self.clicked_materials: list[SearchedMaterialInfo] = []

        self.resources_total: int = 0
        self.licenses: dict[Licenses] = {}
        self.resources_no_title_identifiers: list[MissingInfo] = []
        self.resources_no_subject_identifiers: list[MissingInfo] = []
        self.resources_no_educontext: list[MissingInfo] = []
        self.resources_no_keywords: list[MissingInfo] = []
        self.resources_no_licenses: list[MissingInfo] = []
        self.collection_no_keywords: list[MissingInfo] = []
        self.collection_no_description: list[MissingInfo] = []
        self.quality_score: int = 0
        self._layout = html.Div()

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return self.name

    def as_dict(self):
        self.update_properties()
        return {
            "name": self.name,
            "quality_score": self.quality_score,
            "clicked_materials": len(self.clicked_materials),
            "resources_total": self.resources_total,
            "resources_no_title_identifiers": len(self.resources_no_title_identifiers),
            "resources_no_subject_identifiers": len(self.resources_no_subject_identifiers),
            "resources_no_educontext": len(self.resources_no_educontext),
            "resources_no_keywords": len(self.resources_no_keywords),
            "oer_licenes": self.licenses.get("oer"),
            "resources_no_licenses": len(self.resources_no_licenses),
            "collection_no_keywords": len(self.collection_no_keywords),
            "collection_no_description": len(self.collection_no_description)
        }

    def update_properties(self):
        """
        Updates relevant properties with es-queries.
        """
        self.clicked_materials = oeh.searched_materials_by_collection.get(
            self._id, [])
        self.resources_total = self.get_resources_total()
        self.resources_no_licenses = self.get_missing_attribute(
            None, qtype="license")
        self.resources_no_educontext = self.get_missing_attribute(
            "properties.ccm:educationalcontext", qtype="resource")
        self.resources_no_subject_identifiers = self.get_missing_attribute(
            "properties.ccm:taxonid", qtype="resource")
        self.licenses = self.get_licenses()
        self.resources_no_title_identifiers = self.get_missing_attribute(
            "properties.cclom:title", qtype="resource")
        self.resources_no_keywords = self.get_missing_attribute(
            "properties.cclom:general_keyword", qtype="resource")
        self.collection_no_keywords: list[MissingInfo] = self.get_missing_attribute(
            "properties.cclom:general_keyword", qtype="collection")
        self.collection_no_description: list[MissingInfo] = self.get_missing_attribute(
            "properties.cm:description", qtype="collection")
        self.quality_score = self.calc_quality_score()

    @property
    def layout(self):
        logging.info("update properties")
        self.update_properties()
        logging.info("Setting layout...")
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
                score += ((1 - (len(item) / self.resources_total)) /
                          len(score_items))
            except ZeroDivisionError:
                logging.error(
                    f"Zero Division Error with Collection: {self.name}")
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
        r: list[dict] = oeh.getStatisicCounts(self._id, "properties.ccm:commonlicense_key.keyword").get(
            "aggregations", {}).get("license", {}).get("buckets", [])
        licenses = self.sort_licenses(r)
        return licenses

    def get_resources_total(self):
        r: int = oeh.getStatisicCounts(self._id).get(
            "hits", {}).get("total", {}).get("value", 0)
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
                                                html.Span(
                                                    f"{i.title if i.title else i.name}"),
                                                html.Img(
                                                    src=ES_PREVIEW_URL.format(i._id)),
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
        clicked_materials = []  # table elements
        search_term_count = "\"{}\" ({})"  # term, count

        if not materials:
            return html.Div()

        header_row = html.Div(
            className="searched-material-row",
            children=[
                html.Span("Titel"),
                html.Span("Suchbegriff(e)"),
                html.Span("Klicks")
            ],
            style={"margin-bottom": "20px"}
        )
        clicked_materials.append(header_row)
        for material in materials:
            search_term_comprehension = " ".join([search_term_count.format(
                term, count) for term, count in material.search_strings.items()])
            clicked_materials.append(
                html.P(
                    children=[
                        html.A(
                            children=[
                                html.Div(
                                    className="searched-material-row",
                                    children=[
                                        html.Span(
                                            f"{material.title if material.title else material.name}"),
                                        html.Span(
                                            f"{search_term_comprehension}"),
                                        html.Span(f"{material.clicks}"),
                                        html.Img(
                                            src=ES_PREVIEW_URL.format(material._id))
                                    ]
                                )
                            ],
                            href=f"{ES_NODE_URL.format(material._id, None)}",
                            target="_blank"
                        )
                    ]
                )
            )
        return html.Div(
            className="searched-material-box",
            children=[
                html.P(f"{title}"),
                html.Div(
                    children=[*clicked_materials],
                    className="card",
                )
            ]
        )

    def build_layout(self):
        res_no_title = self.build_missing_info_card(
            "Materialien ohne Titel", self.resources_no_title_identifiers)
        res_no_subject = self.build_missing_info_card(
            "Materialien ohne Fachzuordnung", self.resources_no_subject_identifiers)
        res_no_educontext = self.build_missing_info_card(
            "Materialien ohne Zuordnung der Bildungstufe", self.resources_no_educontext)
        res_no_keywords = self.build_missing_info_card(
            "Materialien ohne Schlagworte", self.resources_no_keywords)
        res_no_license = self.build_missing_info_card(
            "Materialien ohne Lizenz", self.resources_no_licenses)
        coll_no_keywords = self.build_missing_info_card(
            "Sammlungen ohne Schlagworte", self.collection_no_keywords)
        coll_no_description = self.build_missing_info_card(
            "Sammlung ohne Beschreibungstext", self.collection_no_description)
        searched_materials = self.build_searched_materials(
            "Diese Materialien aus deinem Fachportal wurden gesucht und geklickt (~letze 30 Tage)", self.clicked_materials)
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
            r: list = oeh.getMaterialByMissingAttribute(
                self._id, attribute).get("hits", {}).get("hits", [])
        elif qtype == "collection":
            r: list = oeh.getCollectionByMissingAttribute(
                self._id, attribute).get("hits", {}).get("hits", [])
        elif qtype == "license":
            # some resources don't have a license keyword others have one, but it is NONE, "" or something strange
            # so we need to combine this here
            r1: list = oeh.get_material_by_condition(
                self._id, condition="missing_license").get("hits", {}).get("hits", [])
            r2: list = oeh.getMaterialByMissingAttribute(
                self._id, attribute="properties.ccm:commonlicense_key.keyword").get("hits", {}).get("hits", [])
            r: list = r1 + r2
        else:
            raise ValueError("qtype is not of: collection, resource, license")
        result: list[MissingInfo] = [
            self.parse_result(item, qtype) for item in r]
        return result

    def parse_result(self, resource: dict, qtype: Literal["collection", "resource", "license"]):
        _id = resource.get("_source", {}).get("nodeRef", {}).get("id", None)
        name = resource.get("_source", {}).get(
            "properties", {}).get("cm:name", None)
        title = resource.get("_source", {}).get(
            "properties", {}).get("cclom:title", None)
        _type = resource.get("_source", {}).get("type", None)
        # action hint for edu-sharing to open dialog
        action = "OPTIONS.EDIT"
        if qtype == "license":
            action = "OPTIONS.LICENSE"
        return MissingInfo(_id, name, title, _type, action)

    def make_url(self):
        return self.name.lower().replace(" ", "-").replace("ü", "ue")
