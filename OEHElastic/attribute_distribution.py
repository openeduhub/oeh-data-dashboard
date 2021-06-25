from dataclasses import dataclass

import dash_html_components as html
import plotly.graph_objects as go
import dash_core_components as dcc

import plotly.express as px


import pandas as pd

from OEHElastic import oeh


# define a list of attributes
@dataclass
class Attribute:
    name: str
    es_property: str
    df: pd.DataFrame = None
    graph: dcc.Graph = None


attributes = [
    Attribute("thumbnail", "properties.ccm:thumbnailurl.keyword"),
    Attribute("learning resource type", "i18n.de_DE.ccm:educationallearningresourcetype.keyword"),
    Attribute("duration", "properties.cclom:duration.keyword"),
    Attribute("language", "properties.cclom:general_language.keyword"),
    Attribute("source", "i18n.de_DE.ccm:replicationsource.keyword"),
    # "siegel_redaktion"
    Attribute("oer_label", "i18n.de_DE.ccm:license_oer.keyword"),
    Attribute("license", "properties.ccm:commonlicense_key.keyword"),
    Attribute("title", "properties.cclom:title.keyword"),
    Attribute("description", "properties.cclom:general_description.keyword"),
    Attribute("discipline", "i18n.de_DE.ccm:taxonid.keyword"),
    Attribute("educationalContext (Bildungsstufe)", "i18n.de_DE.ccm:educationalcontext.keyword"),
    Attribute("login", "i18n.de_DE.ccm:conditionsOfAccess.keyword"),
    Attribute("price", "i18n.de_DE.ccm:price.keyword"),
    Attribute("containsAdvertisement", "i18n.de_DE.ccm:containsAdvertisement.keyword"),
    Attribute("Assoziierte Sammlungen", "collections.path.keyword")
]

# get missing + aggregations
def build_attribute_df(attributes: list[Attribute]):
    for attribute in attributes:
        missing_agg = oeh.get_aggregations(attribute.es_property, agg_type="missing")
        missing_bucket = oeh.get_doc_count_from_missing_agg(missing_agg)
        top_ten_and_other_agg = oeh.get_aggregations(attribute.es_property, size=10)
        top_ten_and_other_buckets = oeh.build_buckets_from_agg(top_ten_and_other_agg, include_other=True)
        buckets = [*top_ten_and_other_buckets, missing_bucket]
        # build a dataframe
        attribute.df = oeh.build_df_from_buckets(buckets)

        # build layout
        attribute.graph = build_graph_from_df(attribute)
    return attributes

def build_graph_from_df(attribute: Attribute):
    fig = px.bar(attribute.df, x="key", y="doc_count")
    fig.update_layout(
        title = f"Attribut: {attribute.name} ({attribute.es_property})"
    )
    graph = dcc.Graph(id=f"{attribute.name}", figure=fig)
    return graph

def build_layout(attributes):
    layout = []
    for attribute in attributes:
        layout.append(html.Div(attribute.graph))
    return layout

# plot it
if __name__ == "__main__":
    attributes = build_attribute_df(attributes)
else:
    attributes = build_attribute_df(attributes)
    layout = build_layout(attributes)