from __future__ import absolute_import, division, print_function
from builtins import super, range, zip, round, map
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed, wait
from tqdm import tqdm

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

class ReadLoads:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_loads(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        # self.logger.debug("Start of load")
        database = self.input_file
        conn = self.get_conn()

        Loads = self.read_loads(conn)
        elementColumnNames = self.read_element_column_names(conn)
        for idx, name in enumerate(elementColumnNames):
            if name["name"] == "Element_ID":
                self.elementID = idx
            elif name["name"] == "Type":
                self.elementType = idx
            elif name["name"] == "Name":
                self.elementName = idx
            elif name["name"] == "VoltLevel_ID":
                self.elementVoltLevel = idx

        terminalColumnNames = self.read_terminal_column_names(conn)
        for idx, name in enumerate(terminalColumnNames):
            if name["name"] == "TerminalNo":
                self.terminalNo = idx
            elif name["name"] == "Node_ID":
                self.terminalID = idx
            elif name["name"] == "Flag_Terminal":
                self.terminalPhase = idx
        self.calcParameter = self.read_calcParameter(conn)[0]
        calcParameterNames = self.read_calcParameter_column_names(conn)
        for idx, name in enumerate(calcParameterNames):
            if name["name"] == "ull":
                self.loadVoltageLowerLimit = idx
            elif name["name"] == "uul":
                self.loadVoltageUpperLimit = idx
        loadColumnNames = self.read_load_column_names(conn)
        for idx, name in enumerate(loadColumnNames):
            if name["name"] == "Element_ID":
                self.loadID = idx
            elif name["name"] == "P1":
                self.loadP1 = idx
            elif name["name"] == "P2":
                self.loadP2 = idx
            elif name["name"] == "P3":
                self.loadP3 = idx
            elif name["name"] == "Q1":
                self.loadQ1 = idx
            elif name["name"] == "Q2":
                self.loadQ2 = idx
            elif name["name"] == "Q3":
                self.loadQ3 = idx
            elif name["name"] == "cosphi":
                self.loadPF = idx
            elif name["name"] == "Flag_Variant":
                self.loadFlagVariant = idx
            elif name["name"] == "P":
                self.loadP = idx
            elif name["name"] == "Q":
                self.loadQ = idx
            elif name["name"] == "S":
                self.loadS = idx
            elif name["name"] == "fP":
                self.loadfP = idx
            elif name["name"] == "fQ":
                self.loadfQ = idx
            elif name["name"] == "fS":
                self.loadfS = idx
            elif name["name"] == "Flag_Lf":
                self.loadType = idx
            elif name["name"] == "u":
                self.loadVoltagePercentage = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        self.totalLoads = 0

        for LOAD in tqdm(Loads, desc='Reading Loads', disable=not self.show_progress):
            self.totalLoads = self.totalLoads + 1
            ReadLoads.parse_load(self, LOAD, model)

        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_load(self, LOAD, model):
        current = self.totalLoads
        self.logger.debug(f"Thread {__name__} starting %s", self.totalLoads)
        # create a database connection
        database = self.input_file
        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1
        # self.logger.debug(conn)
        # self.logger.debug("START PARSING LINES")

        if LOAD[self.loadFlagVariant] == 1:
            element = self.read_element(conn, LOAD[self.loadID])[0]
            terminal = self.read_terminal(conn, element[self.loadID])[0]
            voltLevel = self.read_voltageLevel(
                conn, element[self.elementVoltLevel]
            )[0]
            if voltLevel[self.voltageLevelUn] < voltageLevel:
                load = Load(model)
                load.name = str(element[self.elementName]).replace(" ", "")
                self.logger.debug('Parsing load named: ' + load.name)
                if self.transformer == "False" and (
                    self.filter == "LV" or self.filter == "MV"
                ):
                    elements = self.read_terminal_nodeID(
                        conn, terminal[self.terminalID]
                    )
                    key = False
                    for Element in elements:
                        transformers = self.read_twoWindingTransformer(
                            conn, Element[self.elementID]
                        )
                        if len(transformers) > 0:
                            key = True
                    if key == True:
                        load.connecting_element = "sourcebus_" + str(
                            round(voltLevel[self.voltageLevelUn] * 10 ** 3)
                        )
                    else:
                        load.connecting_element = str(terminal[self.terminalID])
                else:
                    load.connecting_element = str(terminal[self.terminalID])
                # self.logger.debug(voltLevel[6])
                load.nominal_voltage = voltLevel[self.voltageLevelUn] * 10 ** 3
                # self.logger.debug(calcParameter[self.loadVoltageLowerLimit] / 100)
                load.vmin = self.calcParameter[self.loadVoltageLowerLimit] / 100
                load.vmax = self.calcParameter[self.loadVoltageUpperLimit] / 100
                # self.logger.debug(terminal[4])
                # self.logger.debug(element[8])
                # row = self.read_loads(conn, element[0])[0]
                row = LOAD
                PLoad = map(
                    lambda x: x * 10 ** 3,
                    [row[self.loadP1], row[self.loadP2], row[self.loadP3]],
                )
                QLoad = map(
                    lambda x: x * 10 ** 3,
                    [row[self.loadQ1], row[self.loadQ2], row[self.loadQ3]],
                )
                LoadPF = row[self.loadPF]  # row[31]
                # self.logger.debug(LoadPF)
                LoadQFactor = row[self.loadfQ]  # (1 - LoadPF ** 2) ** 0.5

                PLoadkva = map(
                    lambda x: x * 10 ** 3,
                    [
                        row[self.loadP1] * LoadPF,
                        row[self.loadP2] * LoadPF,
                        row[self.loadP3] * LoadPF,
                    ],
                )
                # self.logger.debug(ow[self.loadQ1])
                # self.logger.debug(LoadQFactor)
                QLoadkva = map(
                    lambda x: x * 10 ** 3,
                    [
                        row[self.loadQ1] * LoadQFactor,
                        row[self.loadQ2] * LoadQFactor,
                        row[self.loadQ3] * LoadQFactor,
                    ],
                )
                ### Load Phases##################
                #
                # Phases are given as a string "L1 L2 L3 N"
                # Convert this string to a list of characters
                #
                phases = terminal[self.terminalPhase]
                sectionPhases = list()
                # self.logger.debug(phases)
                phaseNumber = 0
                if phases == 1:
                    sectionPhases.append("A")
                    phaseNumber = 1
                    load.nominal_voltage = load.nominal_voltage / math.sqrt(3)
                elif phases == 2:
                    sectionPhases.append("B")
                    phaseNumber = 1
                    load.nominal_voltage = load.nominal_voltage / math.sqrt(3)
                elif phases == 3:
                    sectionPhases.append("C")
                    phaseNumber = 1
                    load.nominal_voltage = load.nominal_voltage / math.sqrt(3)
                elif phases == 4:
                    sectionPhases.append("A")
                    sectionPhases.append("B")
                    phaseNumber = 2
                elif phases == 5:
                    sectionPhases.append("B")
                    sectionPhases.append("C")
                    phaseNumber = 2
                elif phases == 6:
                    sectionPhases.append("A")
                    sectionPhases.append("C")
                    phaseNumber = 2
                elif phases == 7:
                    sectionPhases.append("A")
                    sectionPhases.append("B")
                    sectionPhases.append("C")
                    phaseNumber = 3
                elif phases == 8:
                    sectionPhases.append("N")
                # self.logger.debug(sectionPhases)
                # Set the Phase Loads
                self.logger.debug('Loads nominal voltage: '+str(load.nominal_voltage))

                for P, Q, Pkva, Qkva, phase in zip(
                    PLoad,
                    QLoad,
                    PLoadkva,
                    QLoadkva,
                    sectionPhases,  # ["A", "B", "C"]
                ):
                    # Only create a PhaseLoad is P OR Q is not zero
                    if P != 0 or Q != 0:

                        # Create the PhaseLoad DiTTo object
                        phase_load = PhaseLoad(model)

                        # Set the Phase
                        phase_load.phase = phase

                        # Set P
                        phase_load.p = P

                        # Set Q
                        phase_load.q = Q

                        # Add the PhaseLoad to the list
                        load.phase_loads.append(phase_load)

                    elif Pkva != 0 or Qkva != 0:

                        # Create the PhaseLoad DiTTo object
                        phase_load = PhaseLoad(model)

                        # Set the Phase
                        phase_load.phase = phase
                        # self.logger.debug(phase)

                        # Set P
                        phase_load.p = Pkva

                        # Set Q
                        phase_load.q = Qkva

                        # Add the PhaseLoad to the list
                        load.phase_loads.append(phase_load)

                    else:
                        # if there is no load information, place a small load instead of writing zero to the load
                        phase_load = PhaseLoad(model)

                        # Set the Phase
                        phase_load.phase = phase
                        if row[self.loadType] == 1:
                            # Set P
                            phase_load.p = (
                                row[self.loadP]
                                / phaseNumber
                                * 10 ** 6
                                * row[self.loadfP]
                            )

                            # Set Q
                            phase_load.q = (
                                row[self.loadQ]
                                / phaseNumber
                                * 10 ** 6
                                * row[self.loadfQ]
                            )
                        elif row[self.loadType] == 3:
                            # self.logger.debug(row[self.loadS])
                            s = row[self.loadS] * 10 ** 6 * row[self.loadfS]
                            # self.logger.debug(s)
                            pf = row[self.loadPF]
                            P = pf * s
                            # self.logger.debug(P)
                            phase_load.p = P / phaseNumber
                            phi = math.acos(pf)
                            Q = math.sin(phi) * s
                            # self.logger.debug(Q)
                            phase_load.q = Q / phaseNumber

                        # Add the PhaseLoad to the list
                        load.phase_loads.append(phase_load)
        self.logger.debug(f"Thread {__name__} finishing %s ", current)

    # @log_exceptions
    def parse_LV_Loads(self, model, bus):
        self.logger.info(f"Thread {__name__} starting")
        # self.logger.debug("Start of load")
        database = self.input_file
        conn = self.get_conn()

        elements = self.read_lineTerminalsByNodeID(conn, bus)
        elementColumnNames = self.read_element_column_names(conn)
        for idx, name in enumerate(elementColumnNames):
            if name["name"] == "Element_ID":
                self.elementID = idx
            elif name["name"] == "Type":
                self.elementType = idx
            elif name["name"] == "Name":
                self.elementName = idx
            elif name["name"] == "VoltLevel_ID":
                self.elementVoltLevel = idx

        terminalColumnNames = self.read_terminal_column_names(conn)
        for idx, name in enumerate(terminalColumnNames):
            if name["name"] == "TerminalNo":
                self.terminalNo = idx
            elif name["name"] == "Node_ID":
                self.terminalID = idx
            elif name["name"] == "Flag_Terminal":
                self.terminalPhase = idx
            elif name["name"] == "Element_ID":
                self.terminalElementID = idx
        self.calcParameter = self.read_calcParameter(conn)[0]
        calcParameterNames = self.read_calcParameter_column_names(conn)
        for idx, name in enumerate(calcParameterNames):
            if name["name"] == "ull":
                self.loadVoltageLowerLimit = idx
            elif name["name"] == "uul":
                self.loadVoltageUpperLimit = idx
        loadColumnNames = self.read_load_column_names(conn)
        for idx, name in enumerate(loadColumnNames):
            if name["name"] == "Element_ID":
                self.loadID = idx
            elif name["name"] == "P1":
                self.loadP1 = idx
            elif name["name"] == "P2":
                self.loadP2 = idx
            elif name["name"] == "P3":
                self.loadP3 = idx
            elif name["name"] == "Q1":
                self.loadQ1 = idx
            elif name["name"] == "Q2":
                self.loadQ2 = idx
            elif name["name"] == "Q3":
                self.loadQ3 = idx
            elif name["name"] == "cosphi":
                self.loadPF = idx
            elif name["name"] == "Flag_Variant":
                self.loadFlagVariant = idx
            elif name["name"] == "P":
                self.loadP = idx
            elif name["name"] == "Q":
                self.loadQ = idx
            elif name["name"] == "S":
                self.loadS = idx
            elif name["name"] == "fP":
                self.loadfP = idx
            elif name["name"] == "fQ":
                self.loadfQ = idx
            elif name["name"] == "fS":
                self.loadfS = idx
            elif name["name"] == "Flag_Lf":
                self.loadType = idx
            elif name["name"] == "u":
                self.loadVoltagePercentage = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        self.totalLoads = 0
        for element in elements:
            try:
                LOAD = self.read_load_Element_ID(
                    conn, element[self.terminalElementID]
                )[0]
                self.totalLoads = self.totalLoads + 1
                ReadLoads.parse_load(self, LOAD, model)

            except:
                self.logger.debug("not a load")

        self.logger.debug(f"Thread {__name__} finishing")
