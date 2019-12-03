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
    def __init__(self, rucio_client, globus_client, suitcase_class,
                 directory, file_prefix='{start[uid]}', **kwargs):

        archivable = (suitcase.jsonl.Serializer, suitcase.msgpack.Serializer):

        if suitcase_class not in archivable:
            raise TypeError(f"suitcase_class not in {archivable}")

        self._suitcase = suitcase_class(directory, file_prefix=file_prefix, **kwargs)
        self._rucio_client = rucio_client
        self._globus_client = globus_client
        self._directory = directory
        self._file_prefix = file_prefix
        self._filename = None

    def __call__(self, name, doc):
        self._suitcase(name, doc)
        if name == 'start':
            self._filename = f'{self._file_prefix.format(start=doc)}.msgpack'
        if name == 'stop':
            self.rucio()

    def rucio(self):
        rse_name = 'RUCIOTEST'
        prefix = '/home/msnyder/data/'
        params = {'scheme': 'file', 'prefix': prefix, 'impl': 'rucio.rse.protocols.posix.Default',
                  'third_party_copy': 1, 'domains': {"lan": {"read": 1,"write": 1,"delete": 1},
                                                     "wan": {"read": 1,"write": 1,"delete": 1}}}
        from rucio.client.rseclient import RSEClient
        rseclient = RSEClient()
        p = rseclient.add_protocol(rse_name, params) # p is true on success

        item = [{'path': os.path.join(self._directory, self._filename),
                 'rse': 'RUCIOTEST',
                 'did_scope': 'nsls2', 'force_scheme': 'file',
                 'pfn': 'file:///home/msnyder/data/' + self._filename}

        from rucio.client.uploadclient import UploadClient
        uploadclient = UploadClient()
        r = uploadclient.upload(items = item)
