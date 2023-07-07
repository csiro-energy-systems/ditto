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
import threading

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
from ditto.readers.sincal.exception_logger import log_exceptions


class ReadNodes:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_nodes(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        # self.logger.debug("start node")
        database = self.input_file
        conn = self.get_conn()

        rows = self.read_nodes(conn)
        nodeColumnNames = self.read_nodes_column_names(conn)
        for idx, name in enumerate(nodeColumnNames):
            if name["name"] == "lat":
                self.nodeLat = idx
            elif name["name"] == "lon":
                self.nodeLon = idx
            elif name["name"] == "Flag_Variant":
                self.nodeFlagVariant = idx
            elif name["name"] == "VoltLevel_ID":
                self.nodeVoltLevel = idx
            elif name["name"] == "Name":
                self.nodeName = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        self.totalNodes = 0

        from tqdm import tqdm
        for row in tqdm(rows, desc='Reading nodes', disable=not self.show_progress):
            self.totalNodes = self.totalNodes + 1
            ReadNodes.parse_node(self,row,model)

        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_node(self, row, model):
        current = self.totalNodes
        self.logger.debug(f"Thread {__name__} starting %s", self.totalNodes)
        database = self.input_file
        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1

        if row[self.nodeFlagVariant] == 1:
            voltLevel = self.read_voltageLevel(conn, row[self.nodeVoltLevel])[0]
            if voltLevel[self.voltageLevelUn] < voltageLevel:
                node = Node(model)
                graphicNode = self.read_graphicNode(conn, row[0])[0]
                self.busName = row[4]
                node.name = str(row[0])
                self.logger.debug('Node name: ' + node.name)
                node.nominal_voltage = row[29]
                phases = row[31]
                if phases == 1:
                    node.phases.append("A")
                elif phases == 2:
                    node.phases.append("B")
                elif phases == 3:
                    node.phases.append("C")
                elif phases == 4:
                    node.phases.append("A")
                    node.phases.append("B")
                elif phases == 5:
                    node.phases.append("B")
                    node.phases.append("C")
                elif phases == 6:
                    node.phases.append("A")
                    node.phases.append("C")
                elif phases == 7:
                    node.phases.append("A")
                    node.phases.append("B")
                    node.phases.append("C")
                elif phases == 8:
                    node.phases.append("N")
                # self.logger.debug(node.phases)
                # Set the coordinates
                position = Position(model)
                position.long = row[self.nodeLon]  # graphicNode[13]
                position.lat = row[self.nodeLat]  # graphicNode[14]
                position.x = graphicNode[13]
                position.y = graphicNode[14]
                position.elevation = 0
                node.positions.append(position)

        self.logger.debug(f"Thread {__name__} finishing %s", current)

    @log_exceptions
    def parse_LV_Node(self, model, bus):
        self.logger.info(f"Thread {__name__} starting")
        # self.logger.debug("start node")
        database = self.input_file
        conn = self.get_conn()

        rows = self.read_lineNode(conn, bus)
        nodeColumnNames = self.read_nodes_column_names(conn)
        for idx, name in enumerate(nodeColumnNames):
            if name["name"] == "lat":
                self.nodeLat = idx
            elif name["name"] == "lon":
                self.nodeLon = idx
            elif name["name"] == "Flag_Variant":
                self.nodeFlagVariant = idx
            elif name["name"] == "VoltLevel_ID":
                self.nodeVoltLevel = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        self.totalNodes = 0
        for row in rows:
            self.totalNodes = self.totalNodes + 1
            ReadNodes.parse_node(self, row, model)

        # self.logger.debug("end node")
        self.logger.debug(f"Thread {__name__} finishing")
