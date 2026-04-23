"""dati.gov.it (CKAN) source connector."""

from databricks.labs.community_connector.sources.dati_gov_it.dati_gov_it import (
    DatiGovItLakeflowConnect,
)

__all__ = ["DatiGovItLakeflowConnect"]
