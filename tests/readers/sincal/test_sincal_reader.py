import collections
import os
import tempfile
from pathlib import Path
from typing import Union

import numpy as np
from sqlalchemy import text

from ditto import Store
from ditto.readers.sincal.read import Reader
from ditto.models.power_source import PowerSource
from logging import getLogger
logger = getLogger(__name__)

def convert_msaccess_to_sqlite(mdb_file: Path):
    """
    Adapted from https://stackoverflow.com/questions/53687786/how-can-i-convert-an-ms-access-database-mdb-file-to-an-sqlite
    """
    import pandas_access as mdb
    from sqlalchemy import create_engine
    import sys
    import os

    if not mdb_file.is_file() or not mdb_file.name.endswith(".mdb") or not mdb_file.exists():
        raise ValueError(f"File {mdb_file} is not a valid MS Access file")

    import sqlalchemy as sa
    connection_string = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        fr"DBQ={mdb_file.resolve()};"
        r"ExtendedAnsiSQL=1;"
    )
    connection_url = sa.engine.URL.create(
        "access+pyodbc",
        query={"odbc_connect": connection_string}
    )
    source_engine = sa.create_engine(connection_url)

    sqlite_file = str(mdb_file).replace(".mdb", ".db")
    dest_engine = create_engine(f'sqlite:///{sqlite_file}', echo=False)

    # Listing the tables.
    with source_engine.connect() as conn:
        tables = source_engine.dialect.get_table_names(conn)
        for table in tables:
            print(table)
            # read table data and write into sqllite_engine

    from sqlalchemy import create_engine, MetaData
    from sqlalchemy import Column, Integer, String, Table
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker

    # Create some toy table and fills it with some data
    Base = declarative_base()

    SourceSession = sessionmaker(source_engine)
    DestSession = sessionmaker(dest_engine)

    Base.metadata.create_all(source_engine)

    # Build the schema for the new table
    # based on the columns that will be returned
    # by the query:
    metadata = MetaData(bind=dest_engine)
    columns = [Column(desc['name'], desc['type']) for desc in query.column_descriptions]
    column_names = [desc['name'] for desc in query.column_descriptions]
    table = Table("newtable", metadata, *columns)

    # Create the new table in the destination database
    table.create(dest_engine)

    # Finally execute the query
    destSession = DestSession()
    for row in query:
        destSession.execute(table.insert(row))
    destSession.commit()

def test_sincal_to_opendss():
    data_dir = Path("../../../tests/data/")

    test_networks = {
        # 'NFTS_Representative_19': data_dir / "small_cases/sincal/NFTS_Representative_19/database.mdb",
        'LVFT_Representative_N': data_dir / "small_cases/sincal/LVFT_Representative_N/67088_files/database.db",
    }

    for network_name, db in test_networks.items():
        output_path = f"{tempfile.gettempdir()}/{network_name}"

        # if db.name.endswith(".mdb"):
        #     db = convert_msaccess_to_sqlite(db)

        store_list: list = read_sincal(db, single_threaded=True, show_progress=True, separate_lv_networks=True, lv_filter='LV', merge_identical_lines=True, keep_transformers=True)
        if store_list is not None:
            for store in store_list:
                power_source_names = get_power_sources(store)

                if len(power_source_names) > 0:
                    for idx, sourcebus in enumerate(power_source_names):
                        dss_path = Path(f"{output_path}/{idx}")
                        print(f"Parsed network={network_name}, Index={idx}, Source={sourcebus}")
                        store_to_dss(store, dss_path)
                        assert Path(f"{output_path}/{idx}/Master.dss").exists(), "DSS file not found at expected location"

# def test_plot_sincal():
#     ditto_utils.plot_network(store, sourcebus, f'Network={network_name}, Source={sourcebus}, Trans={store.name}, Bus={store.bus_name}', out_dir / network_name, engine='pyvis')

def store_to_dss(store: Store, out_dir: Union[str,Path]):
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
    models: list = reader.parse(store, single_threaded=single_threaded, show_progress=show_progress) #single-threaded for easier debugging
    for m in models:
        m.source_file = str(db_file.resolve())

    for mdl in models:
        types = [type(m).__name__ for m in mdl.model_store]
        logger.info(f'Finished reading network from {db_file.parent.name} with {len(mdl.models)} models in {datetime.now() - t0}: with {collections.Counter(types)}')

    return models


def get_power_sources(store):
    '''
    Gets a list of power source names from a ditto Store object
    @param store: the store to process
    @return: list of names
    '''
    power_source_names = []
    for obj in store.models:
        if isinstance(obj, PowerSource) and obj.is_sourcebus == 1:
            power_source_names.append(obj.name)

    power_source_names = np.unique(power_source_names)
    return power_source_names


if __name__ == '__main__':
    test_sincal_to_opendss()