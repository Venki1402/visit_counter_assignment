import sys
import logging

logger = logging.getLogger("visit-counter")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(levelname)s:     %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
