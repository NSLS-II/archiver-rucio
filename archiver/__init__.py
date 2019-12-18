import os
import suitcase.jsonl
import suitcase.msgpack

from ._version import get_versions
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.common.utils import adler32

__version__ = get_versions()['version']
del get_versions

class Archiver():
    """
    Serialize bluesky documents and register them with Rucio.
    """
    def __init__(self, suitcase_class=suitcase.msgpack.Serializer,
                 directory='/home/vagrant/globus', file_prefix='{start[uid]}',
                 rse='BLUESKY', scope='bluesky-nsls2', dataset='archive',
                 pfn='globus:///~/globus/', **kwargs):

        archivable = (suitcase.jsonl.Serializer, suitcase.msgpack.Serializer)

        if suitcase_class not in archivable:
            raise TypeError(f"suitcase_class not in {archivable}")

        self._suitcase = suitcase_class(directory, file_prefix=file_prefix, **kwargs)
        self._directory = directory
        self._file_prefix = file_prefix
        self._filenames = []

        self.rse = rse
        self.scope = scope
        self.dataset = dataset
        self.pfn = pfn

    def __call__(self, name, doc):
        self._suitcase(name, doc)
        if name == 'start':
            self._filenames.append(f"{self._file_prefix.format(start=doc)}.msgpack")
        #if name == 'resource':
        #    #TODO: capture the image files that are created.
        #    ...
        if name == 'stop':
            self.rucio_register()

    def rucio_register(self):
        files = []
        dids = []

        for filename in self._filenames:
            file = os.path.join(self._directory, filename)
            size = os.stat(file).st_size
            adler = adler32(file)
            files.append({'scope': self.scope, 'name': filename,
                          'bytes': size, 'adler32': adler,
                          'pfn': self.pfn + filename})
        
        replica_client = ReplicaClient()
        replica_client.add_replicas(rse=self.rse, files=files)
        didclient = DIDClient()
        didclient.add_files_to_dataset(self.scope, self.dataset, files)
