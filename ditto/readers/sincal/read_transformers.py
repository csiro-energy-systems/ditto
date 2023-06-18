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


class ReadTransformers:
    logger = logging.getLogger(__name__)

    # @log_exceptions
    def parse_transformers(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        conn = self.create_connection(database)
        with conn:
            Elements = self.read_elements(conn)
            TwoWindingTransformers = self.read_twoWindingTransformers(conn)
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

            twoWindingColumnNames = self.read_twoWinding_column_names(conn)
            for idx, name in enumerate(twoWindingColumnNames):
                if name["name"] == "Element_ID":
                    self.twoWindingID = idx
                elif name["name"] == "Flag_Variant":
                    self.twoWindingFlagVariant = idx
                elif name["name"] == "Un1":
                    self.twoWindingUn1 = idx
                elif name["name"] == "Un2":
                    self.twoWindingUn2 = idx
                elif name["name"] == "Sn":
                    self.twoWindingSn = idx
                elif name["name"] == "VecGrp":
                    self.twoWindingVecGrp = idx
                elif name["name"] == "roh1":
                    self.twoWindingTap1 = idx
                elif name["name"] == "roh2":
                    self.twoWindingTap2 = idx
                elif name["name"] == "roh3":
                    self.twoWindingTap3 = idx
                elif name["name"] == "rohu":
                    self.twoWindingHighStep = idx
                elif name["name"] == "rohl":
                    self.twoWindingLowStep = idx
                elif name["name"] == "rohm":
                    self.twoWindingSetPoint = idx
                elif name["name"] == "Flag_Z0_Input":
                    self.twoWindingZ0Flag = idx
                elif name["name"] == "X0":
                    self.twoWindingX0 = idx
                elif name["name"] == "uk":  # short circuit voltage
                    self.twoWindinguk = idx
                elif name["name"] == "ur":  # short circuit voltage  ohmic part
                    self.twoWindingur = idx
                elif name["name"] == "Vfe":
                    self.twoWindingVfe = idx

            self.totalTwoWinding = 0
            for twoWinding in tqdm(TwoWindingTransformers, desc='Reading transformers', disable=not self.show_progress):
                self.totalTwoWinding = self.totalTwoWinding + 1
                ReadTransformers.parse_twoWindingTransformer(self, twoWinding, model)

        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_twoWindingTransformer(self, twoWinding, model):
        current = self.totalTwoWinding
        self.logger.debug(f"Thread {__name__} starting %s", self.totalTwoWinding)
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1
        result = "True"
        side1 = "True"
        side2 = "True"

        if self.transformer == True or self.transformer == "True":
            result = (twoWinding[self.twoWindingUn1] < voltageLevel) or (twoWinding[self.twoWindingUn2] < voltageLevel)
            side1 = twoWinding[self.twoWindingUn1] < voltageLevel
            side2 = twoWinding[self.twoWindingUn2] < voltageLevel
        elif self.transformer == False or self.transformer == "False":
            result = (twoWinding[self.twoWindingUn1] < voltageLevel) and (twoWinding[self.twoWindingUn2] < voltageLevel)
        ''' Don't parse the transformer if the -transformer flag was False or the lower-voltage side of it is above the voltage specified by the -filter switch level. '''
        if twoWinding[self.twoWindingFlagVariant] == 1 and result:

            element = self.read_element(conn, twoWinding[self.twoWindingID])[0]
            # terminal = self.read_terminal(conn, element[self.elementID])[0]
            # self.logger.debug("Start of Two Winding Transformer")
            transformer = PowerTransformer(model)

            terminals = self.read_terminal(conn, element[self.elementID])
            transformer.name = (
                element[self.elementName].replace(" ", "").lower()
                + "_"
                + str(twoWinding[self.twoWindingID])
            )
            self.logger.debug("Transformer Name: " + transformer.name)
            phase = terminals[0][self.terminalPhase]
            vectorGroup = twoWinding[self.twoWindingVecGrp]
            for winding in range(2):
                self.logger.debug(f"Vector Group: {self.connectionType(vectorGroup, winding)}, phase: {[phase]}")
                if self.connectionType(vectorGroup, winding) == "D" and phase <= 3:
                    phase = 4

            phases = list()
            # self.logger.debug(phase)
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

            Z12 = (
                ((twoWinding[self.twoWindingUn1] * 10 ** 3) ** 2)
                / (twoWinding[self.twoWindingSn] * 10 ** 6)
                * (twoWinding[self.twoWindingur] / 100)
            )
            # self.logger.debug(Z12)
            transformer.reactances = [Z12]
            transformer.loadloss = (
                (twoWinding[self.twoWindingVfe] * 10 ** 3)
                / (twoWinding[self.twoWindingSn] * 10 ** 6)
                * 100
            )
            YFlag = False
            for winding in range(2):
                # Create a new Winding object
                w = Winding(model)
                # self.logger.debug("winding")
                if winding == 0:
                    vectorGroup = twoWinding[self.twoWindingVecGrp]
                    w.connection_type = self.connectionType(vectorGroup, winding)
                    self.logger.debug("Transformer connection type: " + w.connection_type)
                    if (
                        (self.filter == "LV" or self.filter == "MV")
                        and (phase == 1 or phase == 2 or phase == 3)
                        and w.connection_type == "Y"
                    ):
                        YFlag = True
                        w.nominal_voltage = (
                            round(twoWinding[self.twoWindingUn1] * math.sqrt(3))
                            * 10 ** 3
                        )
                    else:
                        w.nominal_voltage = twoWinding[self.twoWindingUn1] * 10 ** 3
                    w.rated_power = twoWinding[self.twoWindingSn] * 10 ** 6

                if winding == 1:
                    if (self.filter == "LV" or self.filter == "MV") and (
                        phase == 1 or phase == 2 or phase == 3
                    ):
                        w.nominal_voltage = (
                            twoWinding[self.twoWindingUn2] / math.sqrt(3) * 10 ** 3
                        )
                    else:
                        w.nominal_voltage = twoWinding[self.twoWindingUn2] * 10 ** 3
                    w.rated_power = twoWinding[self.twoWindingSn] * 10 ** 6
                    w.connection_type = self.connectionType(vectorGroup, winding)
                    # self.logger.debug(w.connection_type)

                # Create the phase windings
                for p in phases:
                    # Instanciate a PhaseWinding DiTTo object
                    try:
                        phase_winding = PhaseWinding(model)
                    except:
                        raise ValueError(
                            "Unable to instanciate PhaseWinding DiTTo object."
                        )

                    # Set the phase
                    try:
                        phase_winding.phase = p
                    except:
                        pass

                    # Set the tap position
                    try:
                        if winding == 0:
                            phase_winding.tap_position = twoWinding[self.twoWindingTap1]
                        if winding == 1:
                            phase_winding.tap_position = twoWinding[self.twoWindingTap2]
                        if winding == 2:
                            phase_winding.tap_position = twoWinding[self.twoWindingTap3]
                    except:
                        pass
                    # Add the phase winding object to the winding
                    w.phase_windings.append(phase_winding)
                # Add the winding object to the transformer
                transformer.windings.append(w)

            for terminal in terminals:
                if terminal[self.terminalNo] == 1:
                    # Set the From element
                    if side1:
                        transformer.from_element = str(terminal[self.terminalID])
                    else:
                        if (phase == 1 or phase == 2 or phase == 3) and YFlag == True:
                            transformer.from_element = "sourcebus_" + str(
                                round(twoWinding[self.twoWindingUn1] * math.sqrt(3))
                                * 1000
                            )
                        else:
                            transformer.from_element = "sourcebus_" + str(
                                round(twoWinding[self.twoWindingUn1] * 1000)
                            )
                elif terminal[self.terminalNo] == 2:
                    # Set the To element
                    if side2:
                        transformer.to_element = str(terminal[self.terminalID])
                    else:
                        if phase == 1 or phase == 2 or phase == 3:
                            transformer.to_element = "sourcebus_" + str(
                                round(twoWinding[self.twoWindingUn2] * math.sqrt(3))
                                * 1000
                            )
                        else:
                            transformer.to_element = "sourcebus_" + str(
                                round(twoWinding[self.twoWindingUn2] * 1000)
                            )

            try:
                regulator = Regulator(model)
            except:
                raise ValueError("Unable to instanciate Regulator DiTTo object.")

            try:
                regulator.name = (
                    "Reg_"
                    + element[self.elementName].replace(" ", "").lower()
                    + "_"
                    + str(twoWinding[self.twoWindingID])
                )
                self.logger.debug("Regulator Name: " + regulator.name)
            except:
                pass

            try:
                regulator.connected_transformer = (
                    element[self.elementName].replace(" ", "").lower()
                    + "_"
                    + str(twoWinding[self.twoWindingID])
                )
            except:
                raise ValueError("Unable to connect LTC to transformer")

            regulator.ltc = 1
            regulator.highstep = int(twoWinding[self.twoWindingHighStep])  # rohl
            regulator.lowstep = int(twoWinding[self.twoWindingLowStep])  # rohu
            if twoWinding[self.twoWindingSetPoint] != 0:
                regulator.setpoint = int(twoWinding[self.twoWindingSetPoint])
        self.logger.debug(f"Thread {__name__} finishing %s", current)

    @log_exceptions
    def parse_transformer(self, model, bus):
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        conn = self.create_connection(database)
        with conn:
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

            twoWindingColumnNames = self.read_twoWinding_column_names(conn)
            for idx, name in enumerate(twoWindingColumnNames):
                if name["name"] == "Element_ID":
                    self.twoWindingID = idx
                elif name["name"] == "Flag_Variant":
                    self.twoWindingFlagVariant = idx
                elif name["name"] == "Un1":
                    self.twoWindingUn1 = idx
                elif name["name"] == "Un2":
                    self.twoWindingUn2 = idx
                elif name["name"] == "Sn":
                    self.twoWindingSn = idx
                elif name["name"] == "VecGrp":
                    self.twoWindingVecGrp = idx
                elif name["name"] == "roh1":
                    self.twoWindingTap1 = idx
                elif name["name"] == "roh2":
                    self.twoWindingTap2 = idx
                elif name["name"] == "roh3":
                    self.twoWindingTap3 = idx
                elif name["name"] == "rohu":
                    self.twoWindingHighStep = idx
                elif name["name"] == "rohl":
                    self.twoWindingLowStep = idx
                elif name["name"] == "rohm":
                    self.twoWindingSetPoint = idx
                elif name["name"] == "Flag_Z0_Input":
                    self.twoWindingZ0Flag = idx
                elif name["name"] == "X0":
                    self.twoWindingX0 = idx
                elif name["name"] == "uk":  # short circuit voltage
                    self.twoWindinguk = idx
                elif name["name"] == "ur":  # short circuit voltage  ohmic part
                    self.twoWindingur = idx
                elif name["name"] == "Vfe":
                    self.twoWindingVfe = idx

            self.totalTwoWinding = 0
            for element in elements:
                try:
                    twoWinding = self.read_twoWindingTransformer(
                        conn, element[self.terminalElementID]
                    )[0]
                    self.totalTwoWinding = self.totalTwoWinding + 1
                    ReadTransformers.parse_twoWindingTransformer(
                        self, twoWinding, model,
                    )

                except:
                    self.logger.debug("not a transformer")

        self.logger.debug(f"Thread {__name__} finishing")
