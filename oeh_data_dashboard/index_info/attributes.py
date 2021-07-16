from dataclasses import dataclass
import pandas as pd
import dash_core_components as dcc


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