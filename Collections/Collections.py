import logging

import dash_core_components as dcc
import dash_html_components as html
import dash_react_wc
import dash_table
import pandas as pd
from HelperClasses import Bucket
from OEHElastic import EduSharing, oeh

from .Collection import Collection
from .Constants import fpm_icons

logger = logging.getLogger(__name__)


class Collections:
    def __init__(self):
        self.collections: list[Collection] = self.get_collections()
        self.cards_for_index_page = self.build_cards_for_index_page() #cards for index page
        self.pathnames: list[str] = self.build_pathnames() # the pathnames e.g. "/physik"
        self.searched_materials_not_in_collections = oeh.searched_materials_by_collection.get("none")
        self.searched_materials_not_in_collections_layout = html.Div()
        self._admin_page_layout = html.Div()

    def get_oeh_search_analytics(self):
        oeh.get_oeh_search_analytics()
        self.searched_materials_not_in_collections = oeh.searched_materials_by_collection.get("none")
        self.searched_materials_not_in_collections_layout = Collection.build_searched_materials("Geklickte Materialien, die in keinem Fachportal liegen (~letzte 30 Tage)", self.searched_materials_not_in_collections) #searched_materials

    def build_pathnames(self):
        return ["/" + item.app_url for item in self.collections]

    def get_collections(self):
        collections = sorted([Collection(item) for item in EduSharing.get_collections()])
        return collections

    def build_wordcloud(self) -> dash_react_wc:
        """
        Returns an array of dicts(keys: text, value) to build the wordcloud
        """
        words: list[Bucket] = oeh.get_aggregations(
            attribute="searchString.keyword",
            index="oeh-search-analytics",
            size=50)
        wc_words: list[dict] = [item.as_wc() for item in words]
        options = {
            "rotationAngles": [0, 0],
            "rotations": 0,
            "fontSizes": [18, 64]
        }
        wc = dash_react_wc.DashReactWc(
            id='wc',
            label='wlo-wc',
            words=wc_words,
            options=options
        )
        return wc
        
        

    def build_index_page(self):
        wc = self.build_wordcloud()
        index_page = html.Div(
            children=[
                wc,
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

    def build_fp_overview(self):
        # build dataframe
        d = [c.as_dict() for c in self.collections]
        df = pd.DataFrame(d)
        df.rename(columns={
            "name": "Name",
            "quality_score": "Qualitäts-Score",
            "clicked_materials": "Geklickte Materialien aus FP",
            "resources_total": "Materialien gesamt",
            "resources_no_title_identifiers": "Materialien ohne Titel",
            "resources_no_subject_identifiers": "Materialien ohne Fachzuordnung",
            "resources_no_educontext": "Materialien ohne Bildungsstufe",
            "resources_no_keywords": "Materialien ohne Schlagworte",
            "oer_licenses": "Anzahl OER",
            "resources_no_licenses": "Keine Lizenzangabe",
            "collection_no_keywords": "Sammlungen ohne Schlagworte",
            "collection_no_description": "Sammlungen ohne Beschreibung"
        }, inplace=True)

        data_table = dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records'),
            sort_action="native",
            style_table={'height': '300px', 'overflowY': 'auto'}
        )
        return html.Div(
            className="info-row-2",
            children=[
                html.P("Übersicht Fachportale"),
                data_table
            ]
        )


    def build_data_table_for_agg(self, attribute: str, name: str, index: str = "workspace", size: int = 10000):
        agg = oeh.get_aggregations(
            attribute=attribute,
            index=index,
            size=size)
        d = [b.as_dict() for b in agg]
        df = pd.DataFrame(d)
        data_table = dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records'),
            sort_action="native",
            style_table={'height': '300px', 'overflowY': 'auto'},
            export_format="xlsx"
        )
        return html.Div(
            children=[
                html.P(name),
                data_table
                ])


    def build_data_table_crawler(self, name: str):
        data = oeh.sort_searched_materials()
        d = [b.as_dict() for b in data]
        df = pd.DataFrame(d)
        df = df[["title", "search_strings", "clicks", "crawler", "local_timestamp"]]
        df.rename(columns={
            "title": "Titel",
            "clicks": "Klicks",
            "search_strings": "Suchbegriffe",
            "crawler": "Crawler",
            "local_timestamp": "Letzter Click"
        },inplace=True)
        data_table = dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records'),
            sort_action="native",
            style_table={'height': '300px', 'overflowY': 'auto'},
            export_format="xlsx"
        )
        return html.Div(
            className="info-row-2",
            children=[
                html.P(name),
                data_table
            ])


    @property
    def admin_page_layout(self):
        logger.info("Build admin page...")
        fp_data_table = self.build_fp_overview()
        lrt_data_table = self.build_data_table_for_agg(
            attribute="i18n.de_DE.ccm:educationallearningresourcetype.keyword",
            name="Learning Resource Typen")
        widget_data_table = self.build_data_table_for_agg(
            attribute="i18n.de_DE.ccm:oeh_widgets.keyword",
            name="Widget Typen"
            )
        creator_data_table = self.build_data_table_for_agg(
            attribute="properties.cm:creator.keyword",
            name="Uploads der FPs"
        )
        most_searched_term_data_table = self.build_data_table_for_agg(
            attribute="searchString.keyword",
            name="Meist gesuchter Begriff",
            index="oeh-search-analytics",
            size=1000
        )
        cralwer_data_table = self.build_data_table_crawler("Geklickte Materialien nach Quellen (letzte 30 Tage)")


        return html.Div(children=[
            fp_data_table,
            cralwer_data_table,
            most_searched_term_data_table,
            html.Div(
                className="info-row-1",
                children=[
                    creator_data_table,
                ]
            ),
            html.Div(
                className="info-row-0",
                children=[
                    lrt_data_table,
                    widget_data_table,
                    ]
            ),
        ])


if __name__ == "__main__":
    C = Collections()
    logger.info(C.collections)
    C.collections[0].layout
    C.admin_page_layout

# %%
else:
    C = Collections()
