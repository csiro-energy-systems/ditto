from __future__ import absolute_import, division, print_function

import collections
import sys
import tempfile
from logging import getLogger
from pathlib import Path
from typing import Union

import opendssdirect as dss
import pandas as pd
import pytest
from black import find_project_root
from opendssdirect import _utils
from opendssdirect._utils import get_string
from opendssdirect.utils import run_command

from ditto import Store
from ditto.readers.sincal.read import Reader
from ditto.visualisation import vis_utils
from ditto.visualisation.vis_utils import get_power_sources

logger = getLogger()
logger.setLevel("INFO")

root_dir = find_project_root('.')[0]
data_dir = Path(root_dir, "tests/data/")

class TestSincalReader:

    @pytest.mark.skip("Need to confirm releasable Sincal/Sqlite data to include for this test")
    def test_sincal_sqllite_to_opendss(self):

        test_networks = {
            'LVFT-Network-K': data_dir / "big_cases/sincal/LVFT-Network-K/database.db",
        }

        for network_name, db in test_networks.items():
            output_path = f"{tempfile.gettempdir()}/{network_name}"

            store_list: list = read_sincal(db, single_threaded=True, show_progress=True, separate_lv_networks=True, lv_filter='LV', merge_identical_lines=True, keep_transformers=True)
            if store_list is not None:
                for store in store_list:
                    power_source_names = get_power_sources(store)

                    if len(power_source_names) > 0:
                        for idx, sourcebus in enumerate(power_source_names):
                            dss_path = Path(f"{output_path}/{idx}")
                            store_to_dss(store, dss_path)
                            assert Path(f"{output_path}/{idx}/Master.dss").exists(), "DSS file not found at expected location"
                            logger.info(f"Parsed network={network_name}, Index={idx}, Source={sourcebus}.  Saved to {dss_path}")

                    vis_utils.plot_network(store, sourcebus, f'Network={network_name}, Source={sourcebus}', Path(f"{output_path}/{network_name}"), engines=['pyvis', 'plotly', 'networkx'])

    @pytest.mark.skipif(not sys.platform.startswith("win"), reason="Sincal Access DBs currently only supported by sqlalchemy-access on Windows")
    def test_sincal_access_to_opendss(self):
        """
        Reads a Sincal Access DB to a Ditto Store and saves it to a DSS file
        Note: this currently only works in Windows, due to some native dependencies on the `sqlalchemy-access` library.
        """
        test_networks = {
            'NFTS_Representative_19': data_dir / "small_cases/sincal/NFTS_Representative_19/database.mdb",
        }

        # Uncomment to convert data from the National Feeder Taxonomy Study (NFTS) - see https://doi.org/10.4225/08/5631B1DF6F1A0
        # mdb_files = list(Path("path/to/DAP-15331 National Feeder Taxonomy Study (NFTS)/DataRelease/FeederModels/Sincal").glob("**/*.mdb"))
        # test_networks = {f.parent.name.replace('_files', ''): f for f in mdb_files}

        for network_name, db in test_networks.items():
            logger.info(f"Reading network: {network_name} from {db}")
            output_path = f"{tempfile.gettempdir()}/{network_name}"

            store_list: list = read_sincal(db, single_threaded=True, show_progress=True, separate_lv_networks=False, lv_filter='MV', merge_identical_lines=True, keep_transformers=True)
            store = store_list[0]
            sourcebus = get_power_sources(store)[0]

            vis_utils.plot_network(store, sourcebus, f'Network={network_name}, Source={str(sourcebus).strip()}', Path(f"{output_path}"), engines=['pyvis', 'plotly', 'networkx'], show_plot=False)
            store_to_dss(store, output_path)

            assert Path(f"{output_path}/Master.dss").exists(), "DSS file not found at expected location"


def store_to_dss(store: Store, out_dir: Union[str, Path]):
    """ Writes a ditto model to a DSS format """
    from ditto.writers.opendss.write import Writer
    Writer(output_path=f"{out_dir}", log_file=f"{out_dir}/conversion.log").write(store)

def read_sincal(db_file: Path, single_threaded=True, show_progress=True, separate_lv_networks=True, lv_filter='LV', merge_identical_lines=True, keep_transformers=True):
    """
    Reads a sincal file into one (or multiple) ditto Store models.

    @param db_file:
    @param single_threaded:
    @param show_progress:
    @param separate_lv_networks: splits the file into separate LV networks by selecting every node/wire on the LV side of each transformer (greedily, but avoiding overlaps). TODO: Somehow takes breaker states into account also, clarify this.
    @param lv_filter: can be set to ‘LV’ or ‘MV’, results in the extraction of only some parts of the network when also using -separate.
        This parameter can take a value LV (low voltage) or MV (high voltage) and determines whether you are allowing through only the
        low voltage and below part of the network, or only the medium voltage and above part of the network.
    @param merge_identical_lines: Determines whether contiguous Lines with similar properties are merged into a single longer line.
    @param keep_transformers: Boolean, determines, whether the transformers that connect the MV and LV sides of the network are included in the -filtered and -separated results or not.
    @return: a list of Stores (models)
    """
    store = Store()
    from datetime import datetime
    t0 = datetime.now()

    logger.info(f'Started reading file: {db_file}')
    reader = Reader(input_file=db_file.resolve(), separate=separate_lv_networks, filter=lv_filter, merge=merge_identical_lines, transformer=keep_transformers)
    models: list = reader.parse(store, single_threaded=single_threaded, show_progress=show_progress) # single-threaded for easier debugging
    for m in models:
        m.source_file = str(db_file.resolve())

    for mdl in models:
        types = [type(m).__name__ for m in mdl.model_store]
        logger.info(f'Finished reading network from {db_file.parent.name} with {len(mdl.models)} models in {datetime.now() - t0}: with {collections.Counter(types)}')

    return models

if __name__ == '__main__':
    TestSincalReader().test_sincal_sqllite_to_opendss()
    # TestSincalReader().test_sincal_access_to_opendss()