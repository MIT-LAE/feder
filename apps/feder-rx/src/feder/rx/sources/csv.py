from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from feder.server import Config


class CSVSource:
    NAME = 'csv'

    def __init__(self, config: 'Config'):
        self.config = config

    def check(self):
        return 'CHECKED!'
