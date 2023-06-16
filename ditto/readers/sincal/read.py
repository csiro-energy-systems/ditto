from __future__ import absolute_import, division, print_function
from builtins import super, range, zip, round, map
import logging
import math
import sys
import os
import json
import cmath
import sqlite3
from sqlite3 import Error
import math
import numpy as np
import time
import datetime
import threading
from tqdm import tqdm

from ..abstract_lv_reader import AbstractLVReader

logger = logging.getLogger(__name__)

from ditto.readers.abstract_reader import AbstractReader
from ditto.store import Store
from ditto.models.node import Node
from ditto.models.line import Line
from ditto.models.load import Load
from ditto.models.phase_load import PhaseLoad
from ditto.models.position import Position
from ditto.models.power_source import PowerSource
from ditto.models.powertransformer import PowerTransformer
from ditto.models.winding import Winding
from ditto.models.phase_winding import PhaseWinding
from ditto.models.regulator import Regulator
from ditto.models.wire import Wire
from ditto.models.capacitor import Capacitor
from ditto.models.phase_capacitor import PhaseCapacitor
from ditto.models.reactor import Reactor
from ditto.models.phase_reactor import PhaseReactor
from ditto.models.photovoltaic import Photovoltaic

from .read_lines import ReadLines

from .read_lines_v2 import ReadLines_V2
from .read_loads import ReadLoads
from .read_nodes import ReadNodes
from .read_sources import ReadSources
from .read_transformers import ReadTransformers
from .read_capacitors import ReadCapacitors
from .read_reactors import ReadReactors
from .read_photovoltaics import ReadPhotovoltaics

# ditto-cli convert --from="sincal" --to="opendss" --input="../../../PSS Files/Sincal/Network/ExampleNetwork_files/database.db" --output="./"


class Reader(AbstractLVReader):
    format_name = 'sincal'

    def create_connection(self, db_file):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            self.logger.debug(f'Error creating sqlite connection to {db_file}', exc_info=1)

        return conn

    def read_lineTerminals(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Terminal WHERE Element_ID=?", (element_ID,))
        rows = cur.fetchall()
        return rows

    def read_lineTerminalsByNodeID(self, conn, node_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Terminal WHERE Node_ID=?", (node_ID,))
        rows = cur.fetchall()
        return rows

    def read_lineTerminalsByElementID(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Terminal WHERE Element_ID=?", (element_ID,))
        rows = cur.fetchall()
        return rows

    def read_lineNode(self, conn, node_ID):
        self.logger.debug(f'Reading line node {node_ID}')
        cur = conn.cursor()
        cur.execute("SELECT * FROM Node WHERE Node_ID=?", (node_ID,))
        rows = cur.fetchall()
        return rows

    def read_elements(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Element")
        rows = cur.fetchall()
        return rows

    def read_elementLines(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Element WHERE Type=?", ("Line",))
        rows = cur.fetchall()
        return rows

    def read_element(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Element WHERE Element_ID=?", (element_ID,))
        row = cur.fetchall()
        return row

    def read_element_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Element)")
        rows = cur.fetchall()
        return rows

    def read_terminal_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Terminal)")
        rows = cur.fetchall()
        return rows

    def read_breaker_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Breaker)")
        rows = cur.fetchall()
        return rows

    def read_line_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Line)")
        rows = cur.fetchall()
        return rows

    def read_lines(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Line")
        rows = cur.fetchall()
        return rows

    def read_line(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Line WHERE Element_ID=?", (element_ID,))
        row = cur.fetchall()
        return row

    def read_breaker(self, conn, terminal_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Breaker WHERE Terminal_ID=?", (terminal_ID,))
        row = cur.fetchall()
        return row

    def read_voltageLevel_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(VoltageLevel)")
        rows = cur.fetchall()
        return rows

    def read_infeeder_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Infeeder)")
        rows = cur.fetchall()
        return rows

    def read_nodes(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Node")

        rows = cur.fetchall()
        return rows

    def read_nodes_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Node)")
        rows = cur.fetchall()
        return rows

    def read_graphicNode(self, conn, node_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM GraphicNode WHERE Node_ID=?", (node_ID,))
        rows = cur.fetchall()
        return rows

    def read_loads(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Load")
        rows = cur.fetchall()
        return rows

    def read_load_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Load)")
        rows = cur.fetchall()
        return rows

    def read_load_Element_ID(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Load WHERE Element_ID=?", (element_ID,))
        rows = cur.fetchall()
        return rows

    def read_calcParameter(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM CalcParameter")
        rows = cur.fetchall()
        return rows

    def read_calcParameter_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(CalcParameter)")
        rows = cur.fetchall()
        return rows

    def read_infeederSource(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Infeeder WHERE Element_ID=?", (element_ID,))
        rows = cur.fetchall()
        return rows

    def read_voltageLevel(self, conn, voltLevel_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM VoltageLevel WHERE VoltLevel_ID=?", (voltLevel_ID,))
        row = cur.fetchall()
        return row

    def read_voltageLevels(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM VoltageLevel")
        row = cur.fetchall()
        return row

    def read_terminal(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Terminal WHERE Element_ID=?", (element_ID,))
        rows = cur.fetchall()
        return rows

    def read_terminal_nodeID(self, conn, node_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Terminal WHERE Node_ID=?", (node_ID,))
        rows = cur.fetchall()
        return rows

    def read_infeeders(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Infeeder")
        rows = cur.fetchall()
        return rows

    def read_manipulation(self, conn, mpl_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM Manipulation WHERE Mpl_ID=?", (mpl_ID,))
        rows = cur.fetchall()
        return rows

    def read_manipulation_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(Manipulation)")
        rows = cur.fetchall()
        return rows

    def read_synchronousMachines(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM SynchronousMachine")
        rows = cur.fetchall()
        return rows

    def read_synchronousMachines_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(SynchronousMachine)")
        rows = cur.fetchall()
        return rows

    def read_twoWindingTransformer(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM TwoWindingTransformer WHERE Element_ID=?", (element_ID,)
        )
        row = cur.fetchall()
        return row

    def read_twoWindingTransformers(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM TwoWindingTransformer")
        row = cur.fetchall()
        return row

    def read_twoWinding_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(TwoWindingTransformer)")
        rows = cur.fetchall()
        return rows

    def read_threeWindingTransformer(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM ThreeWindingTransformer WHERE Element_ID=?", (element_ID,)
        )
        row = cur.fetchall()
        return row

    def read_serialCondensator(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM SerialCondensator WHERE Element_ID=?", (element_ID,))
        row = cur.fetchall()
        return row

    def read_shuntCondensators(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM ShuntCondensator")
        row = cur.fetchall()
        return row

    def read_shuntCondensator(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM ShuntCondensator WHERE Element_ID=?", (element_ID,))
        row = cur.fetchall()
        return row

    def read_shuntCondensator_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(ShuntCondensator)")
        rows = cur.fetchall()
        return rows

    def read_serialReactor(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM SerialReactor WHERE Element_ID=?", (element_ID,))
        row = cur.fetchall()
        return row

    def read_shuntReactor(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM ShuntReactor WHERE Element_ID=?", (element_ID,))
        row = cur.fetchall()
        return row

    def read_dcInfeeder(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM DCInfeeder")
        rows = cur.fetchall()
        return rows

    def read_dcInfeeder_Element_ID(self, conn, element_ID):
        cur = conn.cursor()
        cur.execute("SELECT * FROM DCInfeeder WHERE Element_ID=?", (element_ID,))
        rows = cur.fetchall()
        return rows

    def read_dcInfeeder_column_names(self, conn):
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(DCInfeeder)")
        rows = cur.fetchall()
        return rows

    def connectionType(self, vectorGroup, winding):
        connection = None
        if vectorGroup == 1 and winding == 0:
            connection = "D"
        if vectorGroup == 2 and winding == 0:
            connection = "D"
        if vectorGroup == 3 and winding == 0:
            connection = "D"
        if vectorGroup == 4 and winding == 0:
            connection = "Y"
        if vectorGroup == 5 and winding == 0:
            connection = "Y"
        if vectorGroup == 6 and winding == 0:
            connection = "Y"
        if vectorGroup == 7 and winding == 0:
            connection = "Y"
        if vectorGroup == 8 and winding == 0:
            connection = "Z"
        if vectorGroup == 9 and winding == 0:
            connection = "Z"
        if vectorGroup == 10 and winding == 0:
            connection = "D"
        if vectorGroup == 11 and winding == 0:
            connection = "D"
        if vectorGroup == 12 and winding == 0:
            connection = "D"
        if vectorGroup == 13 and winding == 0:
            connection = "Y"
        if vectorGroup == 14 and winding == 0:
            connection = "Y"
        if vectorGroup == 15 and winding == 0:
            connection = "Y"
        if vectorGroup == 16 and winding == 0:
            connection = "Y"
        if vectorGroup == 17 and winding == 0:
            connection = "Y"
        if vectorGroup == 18 and winding == 0:
            connection = "Z"
        if vectorGroup == 19 and winding == 0:
            connection = "Z"
        if vectorGroup == 20 and winding == 0:
            connection = "Z"
        if vectorGroup == 21 and winding == 0:
            connection = "Z"
        if vectorGroup == 22 and winding == 0:
            connection = "Z"
        if vectorGroup == 23 and winding == 0:
            connection = "D"
        if vectorGroup == 24 and winding == 0:
            connection = "D"
        if vectorGroup == 25 and winding == 0:
            connection = "Y"
        if vectorGroup == 26 and winding == 0:
            connection = "Y"
        if vectorGroup == 27 and winding == 0:
            connection = "Y"
        if vectorGroup == 28 and winding == 0:
            connection = "Y"
        if vectorGroup == 29 and winding == 0:
            connection = "Y"
        if vectorGroup == 30 and winding == 0:
            connection = "Y"
        if vectorGroup == 31 and winding == 0:
            connection = "Z"
        if vectorGroup == 32 and winding == 0:
            connection = "Z"
        if vectorGroup == 33 and winding == 0:
            connection = "Z"
        if vectorGroup == 34 and winding == 0:
            connection = "Z"
        if vectorGroup == 35 and winding == 0:
            connection = "D"
        if vectorGroup == 36 and winding == 0:
            connection = "D"
        if vectorGroup == 37 and winding == 0:
            connection = "D"
        if vectorGroup == 38 and winding == 0:
            connection = "Y"
        if vectorGroup == 39 and winding == 0:
            connection = "Y"
        if vectorGroup == 40 and winding == 0:
            connection = "Y"
        if vectorGroup == 41 and winding == 0:
            connection = "Y"
        if vectorGroup == 42 and winding == 0:
            connection = "Z"
        if vectorGroup == 43 and winding == 0:
            connection = "Z"
        if vectorGroup == 44 and winding == 0:
            connection = "D"
        if vectorGroup == 45 and winding == 0:
            connection = "D"
        if vectorGroup == 46 and winding == 0:
            connection = "D"
        if vectorGroup == 47 and winding == 0:
            connection = "D"
        if vectorGroup == 48 and winding == 0:
            connection = "Y"
        if vectorGroup == 49 and winding == 0:
            connection = "Y"
        if vectorGroup == 50 and winding == 0:
            connection = "Y"
        if vectorGroup == 51 and winding == 0:
            connection = "Y"
        if vectorGroup == 52 and winding == 0:
            connection = "Y"
        if vectorGroup == 53 and winding == 0:
            connection = "Z"
        if vectorGroup == 54 and winding == 0:
            connection = "Z"
        if vectorGroup == 55 and winding == 0:
            connection = "Z"
        if vectorGroup == 56 and winding == 0:
            connection = "Z"
        if vectorGroup == 57 and winding == 0:
            connection = "Z"
        if vectorGroup == 58 and winding == 0:
            connection = "D"
        if vectorGroup == 59 and winding == 0:
            connection = "D"
        if vectorGroup == 60 and winding == 0:
            connection = "Y"
        if vectorGroup == 61 and winding == 0:
            connection = "Y"
        if vectorGroup == 62 and winding == 0:
            connection = "Y"
        if vectorGroup == 63 and winding == 0:
            connection = "Y"
        if vectorGroup == 64 and winding == 0:
            connection = "Y"
        if vectorGroup == 65 and winding == 0:
            connection = "Y"
        if vectorGroup == 66 and winding == 0:
            connection = "Z"
        if vectorGroup == 67 and winding == 0:
            connection = "Z"
        if vectorGroup == 68 and winding == 0:
            connection = "Z"
        if vectorGroup == 69 and winding == 0:
            connection = "Z"
        if vectorGroup == 70 and winding == 0:
            connection = "D"
        if vectorGroup == 71 and winding == 0:
            connection = "Y"
        if vectorGroup == 72 and winding == 0:
            connection = "Y"
        if vectorGroup == 73 and winding == 0:
            connection = "D"
        if vectorGroup == 74 and winding == 0:
            connection = "Z"
        if vectorGroup == 75 and winding == 0:
            connection = "Z"
        if vectorGroup == 76 and winding == 0:
            connection = "D"
        if vectorGroup == 77 and winding == 0:
            connection = "D"
        if vectorGroup == 78 and winding == 0:
            connection = "D"
        if vectorGroup == 79 and winding == 0:
            connection = "Y"
        if vectorGroup == 80 and winding == 0:
            connection = "Y"
        if vectorGroup == 81 and winding == 0:
            connection = "Y"

        if vectorGroup == 1 and winding == 1:
            connection = "D"
        if vectorGroup == 2 and winding == 1:
            connection = "Z"
        if vectorGroup == 3 and winding == 1:
            connection = "Z"
        if vectorGroup == 4 and winding == 1:
            connection = "Y"
        if vectorGroup == 5 and winding == 1:
            connection = "Y"
        if vectorGroup == 6 and winding == 1:
            connection = "Y"
        if vectorGroup == 7 and winding == 1:
            connection = "Y"
        if vectorGroup == 8 and winding == 1:
            connection = "D"
        if vectorGroup == 9 and winding == 1:
            connection = "D"
        if vectorGroup == 10 and winding == 1:
            connection = "N"
        if vectorGroup == 11 and winding == 1:
            connection = "Z"
        if vectorGroup == 12 and winding == 1:
            connection = "Z"
        if vectorGroup == 13 and winding == 1:
            connection = "D"
        if vectorGroup == 14 and winding == 1:
            connection = "D"
        if vectorGroup == 15 and winding == 1:
            connection = "Z"
        if vectorGroup == 16 and winding == 1:
            connection = "Z"
        if vectorGroup == 17 and winding == 1:
            connection = "Z"
        if vectorGroup == 18 and winding == 1:
            connection = "D"
        if vectorGroup == 19 and winding == 1:
            connection = "D"
        if vectorGroup == 20 and winding == 1:
            connection = "Y"
        if vectorGroup == 21 and winding == 1:
            connection = "Y"
        if vectorGroup == 22 and winding == 1:
            connection = "Y"
        if vectorGroup == 23 and winding == 1:
            connection = "Y"
        if vectorGroup == 24 and winding == 1:
            connection = "Y"
        if vectorGroup == 25 and winding == 1:
            connection = "D"
        if vectorGroup == 26 and winding == 1:
            connection = "D"
        if vectorGroup == 27 and winding == 1:
            connection = "Z"
        if vectorGroup == 28 and winding == 1:
            connection = "Z"
        if vectorGroup == 29 and winding == 1:
            connection = "Z"
        if vectorGroup == 30 and winding == 1:
            connection = "Z"
        if vectorGroup == 31 and winding == 1:
            connection = "Y"
        if vectorGroup == 32 and winding == 1:
            connection = "Y"
        if vectorGroup == 33 and winding == 1:
            connection = "Y"
        if vectorGroup == 34 and winding == 1:
            connection = "Y"
        if vectorGroup == 35 and winding == 1:
            connection = "D"
        if vectorGroup == 36 and winding == 1:
            connection = "Z"
        if vectorGroup == 37 and winding == 1:
            connection = "Z"
        if vectorGroup == 38 and winding == 1:
            connection = "Y"
        if vectorGroup == 39 and winding == 1:
            connection = "Y"
        if vectorGroup == 40 and winding == 1:
            connection = "Y"
        if vectorGroup == 41 and winding == 1:
            connection = "Y"
        if vectorGroup == 42 and winding == 1:
            connection = "D"
        if vectorGroup == 43 and winding == 1:
            connection = "D"
        if vectorGroup == 44 and winding == 1:
            connection = "Y"
        if vectorGroup == 45 and winding == 1:
            connection = "Y"
        if vectorGroup == 46 and winding == 1:
            connection = "Z"
        if vectorGroup == 47 and winding == 1:
            connection = "Z"
        if vectorGroup == 48 and winding == 1:
            connection = "D"
        if vectorGroup == 49 and winding == 1:
            connection = "D"
        if vectorGroup == 50 and winding == 1:
            connection = "Z"
        if vectorGroup == 51 and winding == 1:
            connection = "Z"
        if vectorGroup == 52 and winding == 1:
            connection = "D"
        if vectorGroup == 53 and winding == 1:
            connection = "D"
        if vectorGroup == 54 and winding == 1:
            connection = "D"
        if vectorGroup == 55 and winding == 1:
            connection = "Y"
        if vectorGroup == 56 and winding == 1:
            connection = "Y"
        if vectorGroup == 57 and winding == 1:
            connection = "Y"
        if vectorGroup == 58 and winding == 1:
            connection = "Y"
        if vectorGroup == 59 and winding == 1:
            connection = "Y"
        if vectorGroup == 60 and winding == 1:
            connection = "D"
        if vectorGroup == 61 and winding == 1:
            connection = "D"
        if vectorGroup == 62 and winding == 1:
            connection = "Z"
        if vectorGroup == 63 and winding == 1:
            connection = "Z"
        if vectorGroup == 64 and winding == 1:
            connection = "Z"
        if vectorGroup == 65 and winding == 1:
            connection = "Z"
        if vectorGroup == 66 and winding == 1:
            connection = "Y"
        if vectorGroup == 67 and winding == 1:
            connection = "Y"
        if vectorGroup == 68 and winding == 1:
            connection = "Y"
        if vectorGroup == 69 and winding == 1:
            connection = "Y"
        if vectorGroup == 70 and winding == 1:
            connection = "Y"
        if vectorGroup == 71 and winding == 1:
            connection = "Y"
        if vectorGroup == 72 and winding == 1:
            connection = "Y"
        if vectorGroup == 73 and winding == 1:
            connection = "D"
        if vectorGroup == 74 and winding == 1:
            connection = "Y"
        if vectorGroup == 75 and winding == 1:
            connection = "Y"
        if vectorGroup == 76 and winding == 1:
            connection = "D"
        if vectorGroup == 77 and winding == 1:
            connection = "D"
        if vectorGroup == 78 and winding == 1:
            connection = "Y"
        if vectorGroup == 79 and winding == 1:
            connection = "Y"
        if vectorGroup == 80 and winding == 1:
            connection = "Y"
        if vectorGroup == 81 and winding == 1:
            connection = "D"

        return connection

    def thread_function(self, name):
        self.logger.info(f"Thread {__name__} %s: starting")
        time.sleep(2)
        self.logger.info(f"Thread {__name__} %s: finishing")

    def get_LV_Transformers(self, model):
        self.logger.info(f'Reading LV Transformers.  Separating LV Networks: {self.separate}, Voltage Filter: {self.filter}')
        self.filter = "LV"
        ReadTransformers.parse_transformers(
            self, model,
        )

    def get_LV_Transformer(self, model, bus):
        ReadTransformers.parse_transformer(self, model, bus)

    def get_LV_Loads(self, model, bus):
        ReadLoads.parse_LV_Loads(self, model, bus)

    def get_LV_Photovoltaics(self, model, bus):
        ReadPhotovoltaics.parse_LV_Photovoltaics(self, model, bus)

    def get_LV_Lines(self, model, bus):
        self.usedBuses = {}
        self.usedLines = {}
        self.usedBuses[bus] = bus
        nextBus = ReadLines.parse_LV_Lines(self, model, bus)
        self.logger.debug(f'Reading LV Lines for bus {nextBus}')
        for Bus in nextBus:
            if not Bus in self.usedBuses.keys():
                self.usedBuses[Bus] = Bus
                self.get_Next_LV_Lines(model, Bus)

    def get_Next_LV_Lines(self, model, bus):
        self.get_LV_Loads(model, bus)
        self.get_LV_Photovoltaics(model, bus)
        self.get_LV_Node(model, bus)
        nextBus = ReadLines.parse_LV_Lines(self, model, bus)
        for Bus in nextBus:
            if not Bus in self.usedBuses:
                self.logger.debug(f'Bus {Bus} is non trivial')
                self.usedBuses[Bus] = Bus
                self.get_Next_LV_Lines(model, Bus)

    def get_LV_Node(self, model, bus):
        ReadNodes.parse_LV_Node(self, model, bus)

    def read_multi_threaded(self, model, show_progress):
        threads = list()
        if self.merge == False:
            x = threading.Thread(target=ReadLines.parse_lines, args=(self, model, show_progress))
            threads.append(x)
            x.start()
        else:
            x = threading.Thread(target=ReadLines_V2.parse_lines, args=(self, model, show_progress))
            threads.append(x)
            x.start()

        x = threading.Thread(target=ReadLoads.parse_loads, args=(self, model, show_progress))
        threads.append(x)
        x.start()

        x = threading.Thread(target=ReadNodes.parse_nodes, args=(self, model, show_progress))
        threads.append(x)
        x.start()

        x = threading.Thread(target=ReadSources.parse_sources, args=(self, model, show_progress))
        threads.append(x)
        x.start()

        x = threading.Thread(
            target=ReadTransformers.parse_transformers, args=(self, model, show_progress)
        )
        threads.append(x)
        x.start()

        x = threading.Thread(
            target=ReadPhotovoltaics.parse_photovoltaics, args=(self, model, show_progress)
        )
        threads.append(x)
        x.start()

        x = threading.Thread(
            target=ReadCapacitors.parse_capacitors, args=(self, model, show_progress)
        )
        threads.append(x)
        x.start()

        x = threading.Thread(target=ReadReactors.parse_reactors, args=(self, model, show_progress))
        threads.append(x)
        x.start()

        for index, thread in enumerate(threads):
            # self.logger.debug("Main    : before joining thread " + index)
            thread.join()
            # self.logger.debug("Main    : thread done " + index)

        return model

    def read_sequential(self, model, show_progress):
        if hasattr(self, 'separate') and self.separate:
            return self.parse_lv_networks(model, show_progress)
        else:
            return self.parse_whole_network(model, show_progress)

    def parse_whole_network(self, model, show_progress):
        '''
        Parses the whole network as a single model
        @param model:
        @param show_progress:
        @return: None
        '''

        if self.merge == False:
            ReadLines.parse_lines(self, model, show_progress)
        else:
            ReadLines_V2.parse_lines(self, model, show_progress)

        ReadLoads.parse_loads(self, model, show_progress)
        ReadNodes.parse_nodes(self, model, show_progress)
        ReadCapacitors.parse_capacitors(self, model, show_progress)
        ReadPhotovoltaics.parse_photovoltaics(self, model, show_progress)
        ReadSources.parse_sources(self, model, show_progress)
        ReadTransformers.parse_transformers(self, model, show_progress)
        ReadReactors.parse_reactors(self, model, show_progress)


    def parse(self, model, **kwargs):
        '''
        Parses the model without any network splitting.
        @param model:
        @param kwargs:
        @return: a single model with all parsed network elements, or a list of sub-models if separate=True was specified in kwargs.
        '''

        startTime = time.time()
        if "verbose" in kwargs and isinstance(kwargs["verbose"], bool):
            self.verbose = kwargs["verbose"]
        else:
            self.verbose = False

        self.show_progress = False if kwargs.get('show_progress') is None else kwargs.get('show_progress')

        ''' Enable single threading for debug purposes.  Multi-threaded by default.'''
        # if "single_threaded" in kwargs and kwargs['single_threaded'] == True:
        models_or_models = self.read_sequential(model, self.show_progress)
        # else:
        #     self.read_multi_threaded(model,  self.show_progress) # About 5x speedup, not sure of accuracy though


        # Call parse method of abstract reader
        super(Reader, self).parse(model, **kwargs)
        self.logger.debug(
            "Total Read Time: %s", datetime.timedelta(seconds=(time.time() - startTime))
        )
        return models_or_models

    def __init__(self, **kwargs):
        '''
        Initialises the Sincal file reader
        :param input_file: the full path to database.db -an sqllite database file containing Sincal data.
        :param filter: (or -filter from cmdline) can be set to ‘LV’ or ‘MV’, results in the extraction of only some parts of the network when also using -separate.
        This parameter can take a value LV (low voltage) or MV (high voltage) and determines whether you are allowing through only the
        low voltage and below part of the network, or only the medium voltage and above part of the network.
        :param transformer: (-transformer): a Boolean, determines, whether the transformers that connect the MV and LV sides of the network are included in the -filtered and -separated results or not.
        :param separate: (-separate): splits the file into separate LV networks by selecting every node/wire on the LV side of each transformer (greedily, but avoiding overlaps). TODO: Somehow takes breaker states into account also.
        :param merge: (-merge): Determines whether contiguous Lines with similar properties are merged into a single longer line.
        '''
        self.logger = logging.getLogger(__name__)
        self.input_file = kwargs.get("input_file", "./input.glm")

        self.filter = kwargs.get("filter")

        self.transformer = kwargs.get("transformer")

        self.merge = kwargs.get("merge")

        self.separate = kwargs.get("separate")


        super(Reader, self).__init__(**kwargs)


# if __name__ == '__main__':
#    reader = Reader()
