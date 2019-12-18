from .. import Archiver

def test_export(example_data):
    # Exercise the archiver on the myriad cases parametrized in example_data.
    archiver = Archiver(directory='/home/vagrant/globus')
    documents = example_data()
    for name, doc in documents:
        archiver(name, doc)
