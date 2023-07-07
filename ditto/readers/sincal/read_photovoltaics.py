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

class ReadPhotovoltaics:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_photovoltaics(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        conn = self.get_conn()

        # self.logger.debug("start dc infeeder")
        dcInfeeders = self.read_dcInfeeder(conn)
        dcInfeedercolumnNames = self.read_dcInfeeder_column_names(conn)
        manipulationColumnNames = self.read_manipulation_column_names(conn)
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

        for idx, name in enumerate(dcInfeedercolumnNames):
            if name["name"] == "Element_ID":
                self.dcInfeederID = idx
            elif name["name"] == "Flag_Variant":
                self.dcInfeederFlagVariant = idx
            elif name["name"] == "P":
                self.dcInfeederActivePower = idx
            elif name["name"] == "Q":
                self.dcInfeederReactivePower = idx
            elif name["name"] == "fP":
                self.dcInfeederFactorP = idx
            elif name["name"] == "fQ":
                self.dcInfeederFactorQ = idx
            elif name["name"] == "Flag_Lf":
                self.dcInfeederFlagInputType = idx
            elif name["name"] == "cosphi":
                self.dcInfeederPowerFactor = idx
            elif name["name"] == "Umin_Inverter":
                self.dcInfeederUminInverter = idx
            elif name["name"] == "Umax_Inverter":
                self.dcInfeederUmaxInverter = idx
            elif name["name"] == "Mpl_ID":
                self.dcInfeederManipulation = idx

        for idx, name in enumerate(manipulationColumnNames):
            if name["name"] == "Mpl_ID":
                self.manipulationID = idx
            elif name["name"] == "Name":
                self.manipulationName = idx
            elif name["name"] == "fP":
                self.manipulationFP = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        self.calcParameter = self.read_calcParameter(conn)[0]
        calcParameterNames = self.read_calcParameter_column_names(conn)
        for idx, name in enumerate(calcParameterNames):
            if name["name"] == "ull":
                self.loadVoltageLowerLimit = idx
            elif name["name"] == "uul":
                self.loadVoltageUpperLimit = idx

        self.totaldcInfeeder = 0
        from tqdm import tqdm
        for dcInfeeder in tqdm(dcInfeeders, desc='Reading photovoltaics', disable=not self.show_progress):
            self.totaldcInfeeder = self.totaldcInfeeder + 1
            ReadPhotovoltaics.parse_photovoltaic(self, dcInfeeder, model)

        # self.logger.debug("end dc infeeder")
        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_photovoltaic(self, dcInfeeder, model):
        current = self.totaldcInfeeder
        self.logger.debug(f"Thread {__name__} starting %s", self.totaldcInfeeder)
        database = self.input_file
        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1

        if dcInfeeder[self.dcInfeederFlagVariant] == 1:
            element = self.read_element(conn, dcInfeeder[self.dcInfeederID])[0]
            terminal = self.read_terminal(conn, element[self.dcInfeederID])[0]
            voltLevel = self.read_voltageLevel(
                conn, element[self.elementVoltLevel]
            )[0]
            if voltLevel[self.voltageLevelUn] < voltageLevel:
                load = Load(model)
                load.name = (
                    str(dcInfeeder[self.dcInfeederID])
                    + "_"
                    + element[self.elementName].replace(" ", "").lower()
                )
                self.logger.debug('Photvoltaics name: ' + load.name)
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
                            round(voltLevel[self.voltageLevelUn] * 1000)
                        )
                    else:
                        load.connecting_element = str(terminal[self.terminalID])
                else:
                    load.connecting_element = str(terminal[self.terminalID])  # 4
                # self.logger.debug(voltLevel[6])
                load.nominal_voltage = voltLevel[self.voltageLevelUn] * 10 ** 3
                # self.logger.debug(calcParameter[self.loadVoltageLowerLimit] / 100)
                load.vmin = self.calcParameter[self.loadVoltageLowerLimit] / 100
                load.vmax = self.calcParameter[self.loadVoltageUpperLimit] / 100
                # self.logger.debug(terminal[4])
                # self.logger.debug(element[8])
                # row = self.read_loads(conn, element[0])[0]
                row = dcInfeeder
                PLoad = map(lambda x: x * 10 ** 3, [0, 0, 0],)
                QLoad = map(lambda x: x * 10 ** 3, [0, 0, 0],)
                LoadPF = 1  # row[self.loadPF]  # row[31]
                # self.logger.debug(LoadPF)
                LoadQFactor = row[
                    self.dcInfeederFactorQ
                ]  # (1 - LoadPF ** 2) ** 0.5

                PLoadkva = map(lambda x: x * 10 ** 3, [0, 0, 0,],)
                # self.logger.debug(ow[self.loadQ1])
                # self.logger.debug(LoadQFactor)
                QLoadkva = map(lambda x: x * 10 ** 3, [0, 0, 0,],)
                ### Load Phases##################
                #
                # Phases are given as a string "L1 L2 L3 N"
                # Convert this string to a list of characters
                #
                phases = terminal[self.terminalPhase]
                sectionPhases = list()
                #self.logger.debug('Photovoltaics phases: ' + phases)
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
                        if row[self.dcInfeederFlagInputType] == 1:
                            # Set P
                            phase_load.p = (
                                -row[self.dcInfeederActivePower]
                                * 10 ** 6
                                / phaseNumber
                                * row[self.dcInfeederFactorP]
                            )
                            '''self.logger.debug(load.name)
                            self.logger.debug(row[self.dcInfeederFactorP])
                            self.logger.debug(phase_load.p)'''
                            # Set Q
                            phase_load.q = (
                                -row[self.dcInfeederReactivePower]
                                * 10 ** 6
                                / phaseNumber
                                * row[self.dcInfeederFactorQ]
                            )
                        elif row[self.dcInfeederFlagInputType] == 2:
                            pf = row[self.dcInfeederPowerFactor]
                            #self.logger.debug(-row[self.dcInfeederActivePower])
                            phase_load.p = (
                                -row[self.dcInfeederActivePower]
                                * 10 ** 6
                                / phaseNumber
                                * row[self.dcInfeederFactorP]
                            )
                            phi = math.acos(pf)
                            Q = math.sin(phi) * phase_load.p
                            # self.logger.debug(Q)
                            phase_load.q = -Q  # / phaseNumber

                        # Add the PhaseLoad to the list
                        load.phase_loads.append(phase_load)

        self.logger.debug(f"Thread {__name__} finishing %s", current)

    @log_exceptions
    def parse_LV_Photovoltaics(self, model, bus):
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        conn = self.get_conn()

        # self.logger.debug("start dc infeeder")
        # dcInfeeders = self.read_dcInfeeder(conn)
        elements = self.read_lineTerminalsByNodeID(conn, bus)
        dcInfeedercolumnNames = self.read_dcInfeeder_column_names(conn)
        manipulationColumnNames = self.read_manipulation_column_names(conn)
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

        for idx, name in enumerate(dcInfeedercolumnNames):
            if name["name"] == "Element_ID":
                self.dcInfeederID = idx
            elif name["name"] == "Flag_Variant":
                self.dcInfeederFlagVariant = idx
            elif name["name"] == "P":
                self.dcInfeederActivePower = idx
            elif name["name"] == "Q":
                self.dcInfeederReactivePower = idx
            elif name["name"] == "fP":
                self.dcInfeederFactorP = idx
            elif name["name"] == "fQ":
                self.dcInfeederFactorQ = idx
            elif name["name"] == "Flag_Lf":
                self.dcInfeederFlagInputType = idx
            elif name["name"] == "cosphi":
                self.dcInfeederPowerFactor = idx
            elif name["name"] == "Umin_Inverter":
                self.dcInfeederUminInverter = idx
            elif name["name"] == "Umax_Inverter":
                self.dcInfeederUmaxInverter = idx
            elif name["name"] == "Mpl_ID":
                self.dcInfeederManipulation = idx

        for idx, name in enumerate(manipulationColumnNames):
            if name["name"] == "Mpl_ID":
                self.manipulationID = idx
            elif name["name"] == "Name":
                self.manipulationName = idx
            elif name["name"] == "fP":
                self.manipulationFP = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        self.calcParameter = self.read_calcParameter(conn)[0]
        calcParameterNames = self.read_calcParameter_column_names(conn)
        for idx, name in enumerate(calcParameterNames):
            if name["name"] == "ull":
                self.loadVoltageLowerLimit = idx
            elif name["name"] == "uul":
                self.loadVoltageUpperLimit = idx

        self.totaldcInfeeder = 0

        for element in elements:
            try:
                dcInfeeder = self.read_dcInfeeder_Element_ID(conn, element[self.terminalElementID])[0]
                self.totaldcInfeeder = self.totaldcInfeeder + 1
                ReadPhotovoltaics.parse_photovoltaic(self, dcInfeeder, model,)
            except:
                self.logger.debug("Not a dc feeder")

        # self.logger.debug("end dc infeeder")
        self.logger.debug(f"Thread {__name__} finishing")
