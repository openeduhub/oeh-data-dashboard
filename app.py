import dash
import dash_core_components as dcc
import dash_html_components as html
from dash_html_components.H3 import H3
import logging
from Collections import Collections

logging.basicConfig(level=logging.INFO)

C = Collections()

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

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


def build_index_page(C: Collections):
    index_page = html.Div(
        children=[
            html.Div(className="index-container",
                    children=[
                        *C.cards_for_index_page,
                    ]),
            # html.H3("Allgemein Übersicht", className="row-header"),
            # general_fpm_board.layout
        ]
    )
    return index_page


# Update the index
@ app.callback(dash.dependencies.Output('page-content', 'children'),
               [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname in C.pathnames:
        target_collection = next(collection for collection in C.collections if collection.app_url == pathname.removeprefix("/"))
        target_collection.load_data()
        return target_collection.layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here


if __name__ == "__main__":
    index_page = build_index_page(C)
    app.run_server(host="0.0.0.0", debug=True, port=8050)
