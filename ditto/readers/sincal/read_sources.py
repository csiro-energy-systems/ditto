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


class ReadSources:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_sources(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        conn = self.create_connection(database)
        with conn:
            Infeeders = self.read_infeeders(conn)
            elementColumnNames = self.read_element_column_names(conn)
            for name in elementColumnNames:
                if name[1] == "Element_ID":
                    self.elementID = name[0]
                elif name[1] == "Flag_State":
                    self.elementFlagState = name[0]
                elif name[1] == "Type":
                    self.elementType = name[0]
                elif name[1] == "Name":
                    self.elementName = name[0]
                elif name[1] == "VoltLevel_ID":
                    self.elementVoltLevel = name[0]

            terminalColumnNames = self.read_terminal_column_names(conn)
            for name in terminalColumnNames:
                if name[1] == "TerminalNo":
                    self.terminalNo = name[0]
                elif name[1] == "Node_ID":
                    self.terminalID = name[0]
                elif name[1] == "Flag_Terminal":
                    self.terminalPhase = name[0]

            infeederColumnNames = self.read_infeeder_column_names(conn)
            for name in infeederColumnNames:
                if name[1] == "Element_ID":
                    self.infeederID = name[0]
                elif name[1] == "delta":
                    self.infeederDeta = name[0]
                elif name[1] == "Flag_Variant":
                    self.infeederFlagVariant = name[0]

            voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
            for name in voltageLevelColumnNames:
                if name[1] == "Un":
                    self.voltageLevelUn = name[0]

            synchronousMachines = self.read_synchronousMachines(conn)
            synchronousMachineColumnNames = self.read_synchronousMachines_column_names(
                conn
            )
            for name in synchronousMachineColumnNames:
                if name[1] == "Flag_Variant":
                    self.synchronousMachineFlagVariant = name[0]

            from tqdm import tqdm
            self.totalSynchronousMachines = 0

            for synchronousMachine in tqdm(synchronousMachines, desc='Reading synchronous machines', disable=not self.show_progress):
                self.totalSynchronousMachines = self.totalSynchronousMachines + 1
                ReadSources.parse_synchronousMachine(self, synchronousMachine, model)

            self.totalInfeeders = 0
            for infeeder in tqdm(Infeeders, desc='Reading infeeders', disable=not self.show_progress):
                self.totalInfeeders = self.totalInfeeders + 1
                ReadSources.parse_infeeders(self, infeeder, model)

            ReadSources.set_infeeders(self, model)
        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_synchronousMachine(self, synchronousMachine, model):
        current = self.totalSynchronousMachines
        self.logger.debug(f"Thread {__name__} starting %s", self.totalSynchronousMachines)
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        if self.filter == "LV":
            voltageLevel = 1
        with conn:
            if synchronousMachine[self.synchronousMachineFlagVariant] == 1:
                element = self.read_element(conn, synchronousMachine[self.infeederID])[
                    0
                ]
                if element[self.elementFlagState] == 1:
                    voltLevel = self.read_voltageLevel(
                        conn, element[self.elementVoltLevel]
                    )[0]
                    if voltLevel[self.voltageLevelUn] < voltageLevel:
                        terminal = self.read_terminal(conn, element[self.infeederID])[0]
                        source = PowerSource(model)
                        source.name = str(element[self.elementName])
                        self.logger.debug('Source name: ' + source.name)
                        source.nominal_voltage = (
                            voltLevel[self.voltageLevelUn] * 10 ** 3
                        )
                        source.connecting_element = str(terminal[self.terminalID])
                        phases = terminal[self.terminalPhase]
                        source.is_sourcebus = True
                        if phases == 1:
                            source.phases.append("A")
                        elif phases == 2:
                            source.phases.append("B")
                        elif phases == 3:
                            source.phases.append("C")
                        elif phases == 4:
                            source.phases.append("A")
                            source.phases.append("B")
                        elif phases == 5:
                            source.phases.append("B")
                            source.phases.append("C")
                        elif phases == 6:
                            source.phases.append("A")
                            source.phases.append("C")
                        elif phases == 7:
                            source.phases.append("A")
                            source.phases.append("B")
                            source.phases.append("C")
                        elif phases == 8:
                            source.phases.append("N")

        self.logger.debug(f"Thread {__name__} finishing %s", current)

    @log_exceptions
    def parse_infeeders(self, infeeder, model):
        current = self.totalInfeeders
        self.logger.debug(f"Thread {__name__} starting %s", self.totalInfeeders)
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1
        with conn:
            if infeeder[self.infeederFlagVariant] == 1:
                source = PowerSource(model)
                element = self.read_element(conn, infeeder[self.infeederID])[0]
                if element[self.elementFlagState] == 1:
                    voltLevel = self.read_voltageLevel(
                        conn, element[self.elementVoltLevel]
                    )[0]
                    if voltLevel[self.voltageLevelUn] < voltageLevel:
                        terminal = self.read_terminal(conn, element[self.infeederID])[0]
                        source.name = str(element[self.elementName])
                        self.logger.debug('Source name: ' + source.name)
                        # Set the nominal voltage
                        source.nominal_voltage = (
                            voltLevel[self.voltageLevelUn] * 10 ** 3
                        )  # DiTTo in volts
                        # Set the phases
                        phases = terminal[self.terminalPhase]
                        # self.logger.debug(phases)
                        if phases == 1:
                            source.phases.append("A")
                        elif phases == 2:
                            source.phases.append("B")
                        elif phases == 3:
                            source.phases.append("C")
                        elif phases == 4:
                            source.phases.append("A")
                            source.phases.append("B")
                        elif phases == 5:
                            source.phases.append("B")
                            source.phases.append("C")
                        elif phases == 6:
                            source.phases.append("A")
                            source.phases.append("C")
                        elif phases == 7:
                            source.phases.append("A")
                            source.phases.append("B")
                            source.phases.append("C")
                        elif phases == 8:
                            source.phases.append("N")
                        # Set the sourcebus flag to True
                        source.is_sourcebus = True
                        # Set the connection type
                        source.connection_type = "Y"
                        # Set the angle of the first phase
                        source.phase_angle = infeeder[self.infeederDeta]
                        # Set the connecting element of the source
                        source.connecting_element = str(terminal[self.terminalID])
            if self.filter == "LV" or self.filter == "MV":
                voltLevels = self.read_voltageLevels(conn)
                for voltlevel in voltLevels:
                    source = PowerSource(model)
                    source.name = "sourcebus_" + str(
                        round(voltlevel[self.voltageLevelUn] * 1000)
                    )
                    self.logger.debug('Source name: ' + source.name)
                    # Set the nominal voltage
                    source.nominal_voltage = (
                        voltlevel[self.voltageLevelUn] * 10 ** 3
                    )  # DiTTo in volts
                    # Set the phases
                    source.phases.append("A")
                    source.phases.append("B")
                    source.phases.append("C")
                    # Set the sourcebus flag to True
                    source.is_sourcebus = True
                    # Set the connection type
                    source.connection_type = "Y"
                    # Set the connecting element of the source
                    source.connecting_element = "sourcebus_" + str(
                        round(voltlevel[self.voltageLevelUn] * 1000)
                    )

            self.logger.debug(f"Thread {__name__} finishing %s", current)

    def set_infeeders(self, model):
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1
        with conn:
            if self.filter == "LV" or self.filter == "MV":
                voltLevels = self.read_voltageLevels(conn)
                for voltlevel in voltLevels:
                    if (
                        self.transformer == "True"
                        and voltlevel[self.voltageLevelUn] >= voltageLevel
                    ):
                        source = PowerSource(model)
                        source.name = "sourcebus_" + str(
                            round(voltlevel[self.voltageLevelUn] * 1000)
                        )
                        self.logger.debug('Source Name: ' + source.name)
                        source.nominal_voltage = (
                            voltlevel[self.voltageLevelUn] * 10 ** 3
                        )
                        source.phases.append("A")
                        source.phases.append("B")
                        source.phases.append("C")
                        source.is_sourcebus = True
                        source.connection_type = "Y"
                        source.connecting_element = "sourcebus_" + str(
                            round(voltlevel[self.voltageLevelUn] * 1000)
                        )

                    if (
                        self.transformer == "False"
                        and voltlevel[self.voltageLevelUn] < voltageLevel
                    ):
                        source = PowerSource(model)
                        source.name = "sourcebus_" + str(
                            round(voltlevel[self.voltageLevelUn] * 1000)
                        )
                        self.logger.debug('Source Name: ' + source.name)
                        source.nominal_voltage = (
                            voltlevel[self.voltageLevelUn] * 10 ** 3
                        )
                        source.phases.append("A")
                        source.phases.append("B")
                        source.phases.append("C")
                        source.is_sourcebus = True
                        source.connection_type = "Y"
                        source.connecting_element = "sourcebus_" + str(
                            round(voltlevel[self.voltageLevelUn] * 1000)
                        )
