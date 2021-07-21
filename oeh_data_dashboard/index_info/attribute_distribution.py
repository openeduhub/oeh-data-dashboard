from oeh_data_dashboard.index_info.attributes import Attribute

import dash_html_components as html
import dash_core_components as dcc

import plotly.express as px


from oeh_data_dashboard.index_info.attributes import relevant_attributes
from oeh_data_dashboard.oeh_elastic import oeh, AggQuery


# get missing + aggregations
# TODO write a test that checks that bucket is of len 12
def build_attribute_df(attributes: list[Attribute]):
    for attribute in attributes:
        agg_query = AggQuery(attribute=attribute.es_property, agg_type="missing")

        missing_agg = oeh.get_aggregations(agg_query)
        missing_bucket = oeh.get_doc_count_from_missing_agg(missing_agg)

        # we want to get top ten results, therefore changing size to 10
        agg_query.size = 10
        agg_query.agg_type = "terms"

        top_ten_and_other_agg = oeh.get_aggregations(agg_query)
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


def build_layout(attributes: list[Attribute]):
    layout = []
    for attribute in attributes:
        layout.append(html.Div(attribute.graph))
    return layout


attributes = build_attribute_df(relevant_attributes)
layout = build_layout(relevant_attributes)

