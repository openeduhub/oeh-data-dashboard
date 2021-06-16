import logging
import os

from dotenv import load_dotenv


import dash
import dash_core_components as dcc
import dash_html_components as html

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
            children=[
                html.Div(
                    id="loading-output-1",
                    children=[html.Div(id='page-content')])
                ]
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
    else:
        index_page = C.build_index_page()
        return index_page


if __name__ == "__main__":
    import logging.config
    logging.basicConfig(level=logging.INFO)
    app.run_server(host="0.0.0.0", debug=eval(os.getenv("DEBUG", True)), port=os.getenv("APP_PORT", 8050))
