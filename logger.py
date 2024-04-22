from datetime import datetime
from dateutil import parser

# importing module
import logging

LOG_FILE_NAME = "db2_table.log"

# Create and configure logger
logging.basicConfig(
    filename=LOG_FILE_NAME,
    format="%(asctime)s %(message)s",
    filemode="a",
)

# Creating an object
logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.INFO)


def table_info_logger(table_name: str, row_count: int):
    logger.info(f"{table_name} {row_count}")


entry_date: datetime = parser.parse("2024-01-31 15:36:29", fuzzy=True)
today: datetime = parser.parse("2024-02-28 15:36:28", fuzzy=True)

