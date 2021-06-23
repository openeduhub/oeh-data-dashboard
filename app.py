import logging
import os

import dash
import dash_core_components as dcc
import dash_html_components as html
from dotenv import load_dotenv

from Collections.Collections import C

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

index_page = C.build_index_page()

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
    C.get_oeh_search_analytics()
    if pathname in C.pathnames:
        target_collection = next(collection for collection in C.collections if collection.app_url == pathname.removeprefix("/"))
        return target_collection.layout
    elif pathname == "/admin":
        return C.admin_page_layout
    elif pathname == "/empty_fp":
        return C.empty_collections_layout
    else:
        index_page = C.build_index_page()
        return index_page


@app.callback(
    dash.dependencies.Output('coll-no-content-container', 'children'),
    dash.dependencies.Input('my-slider', 'value'),
    dash.dependencies.Input('url', 'pathname'), prevent_initial_call=True)
def update_output(value, pathname: str):
    target_collection = next(
        collection for collection in C.collections if collection.app_url == pathname.removeprefix("/")
        )
    return target_collection.get_coll_no_content_layout(doc_threshold=int(value))

@app.callback(
    dash.dependencies.Output('empty-fp-output', 'children'),
    dash.dependencies.Input('my-slider-all-fp', 'value'), prevent_initial_call=True)
def update_empty_fp_overview(value):
    return C.get_empty_fp_overview(doc_threshold=int(value))


if __name__ == "__main__":
    import logging.config
    logging.basicConfig(level=logging.INFO)
    app.run_server(host="0.0.0.0", debug=eval(os.getenv("DEBUG", True)), port=os.getenv("APP_PORT", 8050))
