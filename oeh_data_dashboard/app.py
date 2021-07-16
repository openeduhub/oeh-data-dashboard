import os

import dash
import dash_core_components as dcc
import dash_html_components as html
from dotenv import load_dotenv

from oeh_data_dashboard.fachportal_index import F
from oeh_data_dashboard.index_info.attribute_distribution import layout as attr_layout

load_dotenv()

# app stuff
external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
                "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True)
app.title = "WLO Analytics"

index_page = F.build_index_page()

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Loading(
            id="loading-1",
            type="graph",
            fullscreen=True,
            children=[html.Div(id='page-content')]
            )
           ]
        )


# Update the index
@ app.callback(
    dash.dependencies.Output('page-content', 'children'),
    dash.dependencies.Input('url', 'pathname'))
def display_page(pathname: str):
    F.get_oeh_search_analytics()
    if pathname in F.pathnames:
        target_collection = next(collection for collection in F.collections if collection.app_url == pathname.removeprefix("/"))
        return target_collection.layout
    elif pathname == "/admin":
        return F.admin_page_layout
    elif pathname == "/empty_fp":
        return F.empty_collections_layout
    elif pathname == "/attributes":
        return attr_layout
    else:
        index_page = F.build_index_page()
        return index_page


@app.callback(
    dash.dependencies.Output('coll-no-content-container', 'children'),
    dash.dependencies.Input('my-slider', 'value'),
    dash.dependencies.Input('url', 'pathname'), prevent_initial_call=True)
def update_output(value, pathname: str):
    target_collection = next(
        collection for collection in F.collections if collection.app_url == pathname.removeprefix("/")
        )
    target_collection.doc_threshold = int(value)
    return target_collection.get_coll_no_content_layout()

@app.callback(
    dash.dependencies.Output('empty-fp-output', 'children'),
    dash.dependencies.Input('my-slider-all-fp', 'value'), prevent_initial_call=True)
def update_empty_fp_overview(value):
    return F.get_empty_fp_overview(doc_threshold=int(value))


def run():
    import logging.config
    logging.basicConfig(level=logging.INFO)
    app.run_server(host="0.0.0.0", debug=eval(os.getenv("DEBUG", True)), port=os.getenv("APP_PORT", 8050))


if __name__ == "__main__":
    run()
