import os
import suitcase.jsonl
import suitcase.msgpack

from ._version import get_versions
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.common.utils import adler32

__version__ = get_versions()['version']
del get_versions

class Archiver():
    """
    Serialize bluesky documents and register them with Rucio.
    """
    def __init__(self, *, suitcase_class=suitcase.msgpack.Serializer,
                 root='/home/vagrant/globus', file_prefix='{start[uid]}',
                 rse='BLUESKY', scope='bluesky-nsls2',
                 pfn='globus:///~/globus/', **kwargs):

        archivable = (suitcase.jsonl.Serializer, suitcase.msgpack.Serializer)

        if suitcase_class not in archivable:
            raise TypeError(f"suitcase_class not in {archivable}")

        self._suitcase = suitcase_class(root, file_prefix=file_prefix, **kwargs)
        self._root = root
        self._file_prefix = file_prefix
        self._filenames = None

        self.rse = rse
        self.scope = scope
        self.pfn = pfn

    def __call__(self, name, doc):
        self._suitcase(name, doc)
        if name == 'start':
            self._filename = f'{self._file_prefix.format(start=doc)}.msgpack'
        if name == 'resource':
            #TODO: capture the image files that are created.
            ...
        if name == 'stop':
            self.rucio_register()

    def rucio_register(self):
        meta = {}
        files = []

        for filename in self._filenames:
            file = op.path.join(self._root, filename)
            size = os.stat(file).st_size
            adler = adler32(file)
            files.append({'scope': self.scope, 'name': filename,
                          'bytes': size, 'adler32': adler,
                          'pfn': self.pfn + filename})
        replica_client = ReplicaClient()
        return replica_client.add_replicas(rse=rse, files=files)

    def replication_rule(self):
        # creating replication rule to purge replicas when it expires
        ruleclient = RuleClient()
        dids = []
        dids.append({'scope': scope, 'name': name})

        ruleclient.add_replication_rule(
                dids = dids, copies = 1, rse_expression = 'RHEL7_VM',
                lifetime = 86400, account = 'gbischof',
                source_replica_expression = 'BLUESKY', purge_replicas = True,
                comment = 'purge_replicas in 24 hours')
