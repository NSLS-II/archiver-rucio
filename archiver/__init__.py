import event_model
import os
import suitcase.jsonl
import suitcase.msgpack

from ._version import get_versions
from databroker.core import parse_handler_registry, discover_handlers
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.common.utils import adler32

__version__ = get_versions()['version']
del get_versions

class Archiver():
    """
    Serialize filled Bluesky documents and register them with Rucio.

    suitcase_class: type, optional
        This is a msgpack Serializer by default. This will determine the
        type of file that is generated.    
    filler_class: type, optional
        This is Filler by default. It can be a Filler subclass,
        ``functools.partial(Filler, ...)``, or any class that provides the
        same methods as ``DocumentRouter``.
    root_map: dict, optional
        This is passed to Filler or whatever class is given in the
        ``filler_class`` parameter below.
        str -> str mapping to account for temporarily
        moved/copied/remounted files.  Any resources which have a ``root``
        in ``root_map`` will be loaded using the mapped ``root``.
    handler_registry : dict, optional
        This is passed to the Filler or whatever class is given in the
        ``filler_class`` parameter below.
        Maps each 'spec' (a string identifying a given type or external
        resource) to a handler class.
        A 'handler class' may be any callable with the signature::
            handler_class(resource_path, root, **resource_kwargs)
        It is expected to return an object, a 'handler instance', which is also
        callable and has the following signature::
            handler_instance(**datum_kwargs)
        As the names 'handler class' and 'handler instance' suggest, this is
        typically implemented using a class that implements ``__init__`` and
        ``__call__``, with the respective signatures. But in general it may be
        any callable-that-returns-a-callable.
    directory : string, Path, or Manager
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.
        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        interface. See the suitcase documentation at
        https://nsls-ii.github.io/suitcase for details.
   file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``,
        which are populated from the RunStart document. The default value is
        ``{start[uid]}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.
    rse: string, optional
        The Rucio RSE (Rucio Storage Element) name to register the files with.
        A Rucio Storage Element (RSE) is the logical abstraction of a storage 
        system for physical files. It is the smallest unit of storage space 
        addressable within Rucio.
    scope: string, optional
        The Rucio scope. The scope string partitions the namespace into 
        several sub namespaces.
    dataset: string, optional
        The Rucio dataset. Rucio files are grouped into datasets.
    pfn: string, optional
        Physical file name
    """
    def __init__(self, suitcase_class=suitcase.msgpack.Serializer,
                 filler_class=event_model.Filler, root_map=None, handler_registry=None,    
                 directory='/home/vagrant/globus', file_prefix='{start[uid]}',
                 rse='BLUESKY', scope='bluesky-nsls2', dataset='archive',
                 pfn='globus:///~/globus/', **kwargs):

        archivable = (suitcase.jsonl.Serializer, suitcase.msgpack.Serializer)

        if suitcase_class not in archivable:
            raise TypeError(f"suitcase_class not in {archivable}")

        self._suitcase = suitcase_class(directory, file_prefix=file_prefix, **kwargs)
        
        if handler_registry is None:
            handler_registry = discover_handlers()
        self._handler_registry = parse_handler_registry(handler_registry)
        self.handler_registry = event_model.HandlerRegistryView(self._handler_registry)
        self._filler = filler_class(handler_registry=self.handler_registry, root_map=root_map)

        self.rse = rse
        self.scope = scope
        self.dataset = dataset
        self.pfn = pfn

    def __call__(self, name, doc):
        self._suitcase(*self._filler(name, doc))
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

    def register_handler(self, spec, handler, overwrite=False):
        if (not overwrite) and (spec in self._handler_registry):
            original = self._handler_registry[spec]
            if original is handler:
                return
            raise DuplicateHandler(
                f"There is already a handler registered for the spec {spec!r}. "
                f"Use overwrite=True to deregister the original.\n"
                f"Original: {original}\n"
                f"New: {handler}")
        self._handler_registry[spec] = handler

    def deregister_handler(self, spec):
        self._handler_registry.pop(spec, None)
