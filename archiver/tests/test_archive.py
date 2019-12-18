from .. import Archiver

def test_export(tmp_path, example_data):
    # Exercise the archiver on the myriad cases parametrized in example_data.
    archiver = Archiver(directory=tmp_path)
    documents = example_data()
    for name, doc in documents:
        archiver(name, doc)
