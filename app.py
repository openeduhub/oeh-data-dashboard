import logging

import dash
import dash_core_components as dcc
import dash_html_components as html

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
def display_page(pathname):
    if pathname in C.pathnames:
        target_collection = next(collection for collection in C.collections if collection.app_url == pathname.removeprefix("/"))
        
        # TODO call get_search_analytics method to update search results
        C.get_oeh_search_analytics()
        target_collection.load_data()
        return target_collection.layout
    else:
        C.get_oeh_search_analytics()
        index_page = C.build_index_page()
        return index_page


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", debug=True, port=8050)
