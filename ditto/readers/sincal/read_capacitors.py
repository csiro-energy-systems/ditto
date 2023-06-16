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
from tqdm import tqdm

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


class ReadCapacitors:

    @log_exceptions
    def parse_capacitors(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        conn = self.create_connection(database)
        with conn:
            elementColumnNames = self.read_element_column_names(conn)
            for name in elementColumnNames:
                if name[1] == "Element_ID":
                    self.elementID = name[0]
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

            shuntCondensatorColumnNames = self.read_shuntCondensator_column_names(conn)
            for name in shuntCondensatorColumnNames:
                if name[1] == "Element_ID":
                    self.shuntCondensatorID = name[0]
                elif name[1] == "Flag_Terminal":
                    self.shuntCondensatorPhase = name[0]
                elif name[1] == "Sn":
                    self.shuntCondensatorRatedReactivePower = name[0]

            voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
            for name in voltageLevelColumnNames:
                if name[1] == "Un":
                    self.voltageLevelUn = name[0]

            self.totalCapacitors = 0

            ShuntCondensators = self.read_shuntCondensators(conn)
            for shuntCapacitor in tqdm(ShuntCondensators, desc='Reading shunt capacitors', disable=not self.show_progress):
                self.totalCapacitors = self.totalCapacitors + 1
                ReadCapacitors.parse_shunt_capacitor(self, shuntCapacitor, model)

        self.logger.info(f"Thread {__name__} %s: finishing")

    @log_exceptions
    def parse_shunt_capacitor(self, shuntCapacitor, model):
        current = self.totalCapacitors
        self.logger.debug("parse_shunt_capacitor Thread starting %s", self.totalCapacitors)
        # create a database connection
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1
        # self.logger.info(conn)
        # self.logger.info("START PARSING LINES")
        with conn:
            element = self.read_element(conn, shuntCapacitor[self.shuntCondensatorID])[
                0
            ]
            # shuntCapacitor = self.read_shuntCondensator(conn, element[0])[0]
            voltLevel = self.read_voltageLevel(conn, element[self.elementVoltLevel])[0]
            if voltLevel[self.voltageLevelUn] < voltageLevel:
                capacitor = Capacitor(model)
                terminal = self.read_terminal(conn, element[self.elementID])[0]
                # Set the name
                capacitor.name = element[self.elementName].replace(" ", "").lower()
                self.logger.info('Capacitor name: ' + capacitor.name)
                # Set the connecting element
                capacitor.connecting_element = str(terminal[self.terminalID])
                # Reactance
                # capacitor.reactance = shuntCapacitor[5]

                phase = terminal[self.terminalPhase]
                phases = list()
                # self.logger.info(phase)
                if phase == 1:
                    phases.append("A")
                elif phase == 2:
                    phases.append("B")
                elif phase == 3:
                    phases.append("C")
                elif phase == 4:
                    phases.append("A")
                    phases.append("B")
                elif phase == 5:
                    phases.append("B")
                    phases.append("C")
                elif phase == 6:
                    phases.append("A")
                    phases.append("C")
                elif phase == 7:
                    phases.append("A")
                    phases.append("B")
                    phases.append("C")
                # Set the nominal voltage
                # Convert from KV to Volts since DiTTo is in volts
                capacitor.nominal_voltage = (
                    voltLevel[self.voltageLevelUn] * 10 ** 3
                )  # DiTTo in volts
                if len(phases) == 3:
                    capacitor.nominal_voltage * math.sqrt(3)

                # For each phase...
                for p in phases:
                    phaseCapacitor = PhaseCapacitor(model)
                    phaseCapacitor.phase = p
                    if p == "A":
                        phaseCapacitor.var = (
                            float(
                                shuntCapacitor[self.shuntCondensatorRatedReactivePower]
                            )
                            / len(phases)
                            * 10 ** 6
                        )  # Ditto in var
                    if p == "B":
                        phaseCapacitor.var = (
                            float(
                                shuntCapacitor[self.shuntCondensatorRatedReactivePower]
                            )
                            / len(phases)
                            * 10 ** 6
                        )  # Ditto in var
                    if p == "C":
                        phaseCapacitor.var = (
                            float(
                                shuntCapacitor[self.shuntCondensatorRatedReactivePower]
                            )
                            / len(phases)
                            * 10 ** 6
                        )  # Ditto in var

                        # phaseCapacitor.var = (float(shuntCapacitor[self.shuntCondensatorRatedReactivePower]) * math.sqrt(3) * 10 ** 6)  # Ditto in var

                    # self.logger.info(phaseCapacitor.var)
                    capacitor.phase_capacitors.append(phaseCapacitor)
                    self.logger.info(f"Thread {__name__}: finishing")
