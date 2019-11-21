import os
import suitcase.jsonl
import suitcase.msgpack

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

class Archiver():
    """
    Serialize bluesky documents and register them with Rucio.
    """
    def __init__(self, rucio_client, suitcase_class, **kwargs):
        archivable = (suitcase.jsonl.Serializer, suitcase.msgpack.Serializer):

        if suitcase_class not in archivable:
            raise TypeError(f"suitcase_class not in {archivable}")

        self.suitcase = suitcase_class(**kwargs)
        self.rucio_client = rucio_client

    def __call__(self, name, doc):
        self.suitcase(name, doc)
        if name == 'start':
            self.register()

    def register(self):
        filename = os.path.realpath(self.suitcase._outputfile.name)

