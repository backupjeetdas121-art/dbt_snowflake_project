import psycopg2
import snowflake.connector

from extract.utils.config import PG_CONFIG, SF_CONFIG, STAGE_SCHEMA
from extract.utils.logging_config import logger


def get_pg_connection():
    logger.info("Opening PostgreSQL connection")
    logger.debug("PG_CONFIG=%s", {k: v for k, v in PG_CONFIG.items() if k != "password"})
    return psycopg2.connect(**PG_CONFIG)


def get_sf_connection(schema: str | None = None):
    connection_config = dict(SF_CONFIG)
    connection_config["schema"] = schema or connection_config.get("schema") or STAGE_SCHEMA

    logger.info("Opening Snowflake connection")
    logger.debug("SF_CONFIG=%s", {k: v for k, v in connection_config.items() if k != "password"})
    return snowflake.connector.connect(**connection_config)
