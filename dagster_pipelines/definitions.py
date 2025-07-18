from dagster import Definitions

import dagster_pipelines.etl.KPI.definitions as kpi_definations
import dagster_pipelines.etl.BG020.bronze.definitions as bg020_bronze_definitions

import logging
from dagster import get_dagster_logger

# Hook DLT loggers into Dagster
dagster_logger = get_dagster_logger()
dlt_logger = logging.getLogger("dlt")

# Set level to show warnings
dlt_logger.setLevel(logging.WARNING)

# Forward DLT logs to Dagster logger
class DagsterLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        if record.levelno == logging.WARNING:
            dagster_logger.warning(msg)
        elif record.levelno == logging.INFO:
            dagster_logger.info(msg)
        elif record.levelno >= logging.ERROR:
            dagster_logger.error(msg)
        else:
            dagster_logger.debug(msg)

dlt_logger.addHandler(DagsterLogHandler())

defs = Definitions.merge(
    kpi_definations.defs, bg020_bronze_definitions.defs
)