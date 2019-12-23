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

        self.rse = rse
        self.scope = scope
        self.dataset = dataset
        self.pfn = pfn

    def __call__(self, name, doc):
        self._suitcase(name, doc)
        if name == 'stop':
            filenames = []
            for files in self._suitcase.artifacts.values():
                filenames.extend(files)
            self.rucio_register(filenames)

    def rucio_register(self, filenames):
        files = []
        dids = []

        for filename in filenames:
            size = os.stat(str(filename)).st_size
            adler = adler32(str(filename))
            files.append({'scope': self.scope, 'name': str(filename.parts[-1]),
                          'bytes': size, 'adler32': adler,
                          'pfn': self.pfn + str(filename.parts[-1])})
        
        replica_client = ReplicaClient()
        replica_client.add_replicas(rse=self.rse, files=files)
        didclient = DIDClient()
        didclient.add_files_to_dataset(self.scope, self.dataset, files)
