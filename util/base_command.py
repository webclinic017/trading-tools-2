import logging
from abc import ABC, abstractmethod


class BaseCommand(ABC):

    def __init__(self):
        super().__init__()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self):
        pass
