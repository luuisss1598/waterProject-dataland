import logging

def setup_logging():
    """
        Create a logging function in order to reference this module from any other script in the project.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        encoding='utf-8'
    )

    return logging.getLogger(__name__)

logger = setup_logging();


