import os
import suitcase.jsonl
import suitcase.msgpack

from ._version import get_versions
from rucio.client.rseclient import RSEClient
from rucio.client.uploadclient import UploadClient

__version__ = get_versions()['version']
del get_versions

class Archiver():
    """
    Serialize bluesky documents and register them with Rucio.
    """
    def __init__(self, *, suitcase_class=suitcase.msgpack.Serializer,
                 directory, file_prefix='{start[uid]}', **kwargs):

        archivable = (suitcase.jsonl.Serializer, suitcase.msgpack.Serializer)

        if suitcase_class not in archivable:
            raise TypeError(f"suitcase_class not in {archivable}")

        rucio_init()

        self._suitcase = suitcase_class(directory, file_prefix=file_prefix, **kwargs)
        self._directory = directory
        self._file_prefix = file_prefix
        self._filename = None

    def __call__(self, name, doc):
        self._suitcase(name, doc)
        if name == 'start':
            self._filename = f'{self._file_prefix.format(start=doc)}.msgpack'
        if name == 'resource':
            #TODO: capture the image files that are created.
            ...
        if name == 'stop':
            self.rucio_register()

    def rucio_init(self):
        rse_name = 'RUCIOTEST'
        prefix = '/home/msnyder/data/'
        params = {'scheme': 'file', 'prefix': prefix, 'impl': 'rucio.rse.protocols.posix.Default',
                  'third_party_copy': 1, 'domains': {"lan": {"read": 1,"write": 1,"delete": 1},
                                                     "wan": {"read": 1,"write": 1,"delete": 1}}}
        rseclient = RSEClient()
        result = rseclient.add_protocol(rse_name, params) # p is true on success

    def rucio_register(self):
        item = [{'path': os.path.join(self._directory, self._filename),
                 'rse': 'RUCIOTEST',
                 'did_scope': 'nsls2', 'force_scheme': 'file',
                 'pfn': 'file:///home/msnyder/data/' + self._filename}]

        uploadclient = UploadClient()
        result = uploadclient.upload(items = item)
