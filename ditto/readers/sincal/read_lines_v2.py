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
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed, wait
from tqdm import tqdm

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


class ReadLines_V2:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_lines(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        database = (
            self.input_file
        )  # r"C:\Users\ericw\Documents\PSS Files\Sincal\Network\ExampleNetwork_files\database.db"

        self.a = complex(
            math.cos(120 * (math.pi / 180)), math.sin(120 * (math.pi / 180))
        )
        self.a2 = complex(
            math.cos(240 * (math.pi / 180)), math.sin(240 * (math.pi / 180))
        )
        self.A = [[1, 1, 1], [1, self.a2, 1], [1, self.a, self.a2]]
        self.inverseA = [[1, 1, 1], [1, self.a, 1], [1, self.a2, self.a]]

        self.A2 = [[1, 1], [1, self.a2]]
        self.inverseA2 = [[1, 1], [1, self.a]]

        # create a database connection
        conn = self.create_connection(database)
        # self.logger.debug(conn)
        # self.logger.debug("START PARSING LINES")
        with conn:
            # self.logger.debug("Successfully Connected")
            # Elements = self.read_elementLines(conn)
            Lines = self.read_lines(conn)
            elementColumnNames = self.read_element_column_names(conn)
            for idx, name in enumerate(elementColumnNames):
                if name["name"] == "Element_ID":
                    self.elementID = idx
                elif name["name"] == "Type":
                    self.elementType = idx
                elif name["name"] == "Name":
                    self.elementName = idx
                elif name["name"] == "Flag_State":
                    self.elementFlagState = idx
                elif name["name"] == "VoltLevel_ID":
                    self.elementVoltLevel = idx
            terminalColumnNames = self.read_terminal_column_names(conn)
            for idx, name in enumerate(terminalColumnNames):
                if name["name"] == "TerminalNo":
                    self.terminalNo = idx
                elif name["name"] == "Node_ID":
                    self.terminalID = idx
                elif name["name"] == "Terminal_ID":
                    self.terminalIDNumber = idx
                elif name["name"] == "Element_ID":
                    self.terminalElementID = idx
            lineColumnNames = self.read_line_column_names(conn)
            for idx, name in enumerate(lineColumnNames):
                if name["name"] == "Element_ID":
                    self.lineID = idx
                elif name["name"] == "Flag_Variant":
                    self.lineFlagVariant = idx
                elif name["name"] == "l":
                    self.lineLength = idx
                elif name["name"] == "Flag_LineTyp":
                    self.lineType = idx
                elif name["name"] == "q":
                    self.lineCrossSection = idx
                elif name["name"] == "r0":
                    self.lineR0 = idx
                elif name["name"] == "x0":
                    self.lineX0 = idx
                elif name["name"] == "r":
                    self.lineR1 = idx
                elif name["name"] == "x":
                    self.lineX1 = idx
                elif name["name"] == "c":
                    self.lineC1 = idx
                elif name["name"] == "c0":
                    self.lineC0 = idx
                elif name["name"] == "Un":
                    self.lineRatedVoltage = idx

            breakerColumnNames = self.read_breaker_column_names(conn)
            for idx, name in enumerate(breakerColumnNames):
                if name["name"] == "Terminal_ID":
                    self.breakerTerminalID = idx
                elif name["name"] == "Breaker_ID":
                    self.breakerID = idx
                elif name["name"] == "Flag_Variant":
                    self.breakerFlagVariant = idx
                elif name["name"] == "Name":
                    self.breakername = idx
                elif name["name"] == "Flag_Typ":
                    self.breakerType = idx
                elif name["name"] == "Flag_State":
                    self.breakerState = idx

            voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
            for idx, name in enumerate(voltageLevelColumnNames):
                if name["name"] == "Un":
                    self.voltageLevelUn = idx

            self.duplicateElements = {}
            self.totalLines = 0

            for row in tqdm(Lines, desc='Reading V2 Lines', disable=not self.show_progress):
                self.totalLines = self.totalLines + 1
                ReadLines_V2.parse_line(self, row, model)

        self.logger.debug(f"Thread {__name__} finishing")


    @log_exceptions
    def parse_line(self, row, model):
        current = self.totalLines
        # self.logger.info(f"Thread {__name__} starting")
        # create a database connection
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1
        with conn:
            duplicate = False
            try:
                exists = self.duplicateElements[row[self.elementID]]
                duplicate = True
            except:
                duplicate = False

            if row[self.elementID] == 38262:
                self.logger.debug("ELEMENT 38262")
                self.logger.debug(duplicate)

            if (
                duplicate == False
                and row[self.lineFlagVariant] == 1
                # and row[self.lineLength] > 0.001
            ):
                element = self.read_element(conn, row[self.lineID])[0]

                self.voltLevel = self.read_voltageLevel(
                    conn, element[self.elementVoltLevel]
                )[0]
                if (
                    self.voltLevel[self.voltageLevelUn] < voltageLevel
                ):  # and row[self.lineLength] > 0.001:
                    self.line = Line(model)
                    self.line.name = str(element[self.elementName])
                    self.logger.debug(f'Parsing line named {self.line.name}')
                    self.terminals = []

                    self.from_name = ""
                    self.to_name = ""
                    self.line.length = row[self.lineLength] * 1000
                    self.line.nominal_voltage = row[self.lineRatedVoltage]
                    if row[self.lineType] == 1:
                        self.line.line_type = "cable"
                    elif row[self.lineType] == 2:
                        self.line.line_type = "overhead"
                    elif row[self.lineType] == 3:
                        self.line.line_type = "connector"
                    self.line.R0 = row[self.lineR0]
                    self.line.R1 = row[self.lineR1]
                    self.line.X0 = row[self.lineX0]
                    self.line.X1 = row[self.lineX1]
                    self.line.C1 = row[self.lineC1] / 1000
                    self.line.C0 = row[self.lineC0] / 1000
                    self.duplicateElements[row[self.lineID]] = row[self.lineID]
                    if row[self.lineID] == 38262:
                        self.logger.debug("LineV2: adding duplicate -2")
                    Terminals = self.read_lineTerminalsByElementID(conn, row[self.elementID])
                    terminalCount = 0

                    phases = Terminals[0][8]
                    sectionPhases = list()

                    if phases == 1:
                        sectionPhases.append("A")
                    elif phases == 2:
                        sectionPhases.append("B")
                    elif phases == 3:
                        sectionPhases.append("C")
                    elif phases == 4:
                        sectionPhases.append("A")
                        sectionPhases.append("B")
                    elif phases == 5:
                        sectionPhases.append("B")
                        sectionPhases.append("C")
                    elif phases == 6:
                        sectionPhases.append("A")
                        sectionPhases.append("C")
                    elif phases == 7:
                        sectionPhases.append("A")
                        sectionPhases.append("B")
                        sectionPhases.append("C")
                    elif phases == 8:
                        sectionPhases.append("N")

                    for phase in sectionPhases:
                        wire = Wire(model)
                        wire.phase = phase
                        self.line.wires.append(wire)

                    for terminal in Terminals:
                        notLine = True
                        self.attachedLineElement = []
                        terminalCount = terminalCount + 1
                        Breaker = self.read_breaker(
                            conn, terminal[self.terminalIDNumber]
                        )
                        if len(Breaker) > 0:
                            if Breaker[0][self.breakerFlagVariant] == 1:
                                Node_ID = terminal[self.terminalID]

                                BreakerTerminals = self.read_lineTerminalsByNodeID(
                                    conn, Node_ID
                                )
                                if len(BreakerTerminals) > 1:
                                    self.line.is_switch = 1
                                    self.line.is_enabled = Breaker[0][self.breakerState]
                                    self.logger.debug(f"LineV2 breaker state: {Breaker[0][self.breakerState]}")

                        AttachedModules = self.read_terminal_nodeID(
                            conn, terminal[self.terminalID]
                        )
                        lines = 0
                        elementID = ""
                        for AttachedModule in AttachedModules:
                            elementID = AttachedModule[self.terminalElementID]
                            if (
                                row[self.lineID]
                                != AttachedModule[self.terminalElementID]
                                and len(AttachedModules) == 2
                            ):
                                attachedLineElements = self.read_lineTerminals(
                                    conn, AttachedModule[self.terminalElementID]
                                )
                                for attachedLineElement in attachedLineElements:
                                    if (
                                        attachedLineElement[self.terminalNo]
                                        != AttachedModule[self.terminalNo]
                                    ):
                                        self.attachedLineElement.append(
                                            attachedLineElement
                                        )
                                if len(self.attachedLineElement) == 1:
                                    line = self.read_line(
                                        conn,
                                        self.attachedLineElement[0][
                                            self.terminalElementID
                                        ],
                                    )
                                    if len(line) == 1:
                                        notLine = False
                                else:
                                    notLine = True

                        if notLine == False:
                            NewTerminals = self.attachedLineElement[0][self.terminalID]

                            nextLine = self.read_line(
                                conn,
                                self.attachedLineElement[0][self.terminalElementID],
                            )[0]
                            if (
                                self.line.R0 == nextLine[self.lineR0]
                                and self.line.R1 == nextLine[self.lineR1]
                                and self.line.X0 == nextLine[self.lineX0]
                                and self.line.X1 == nextLine[self.lineX1]
                            ) or row[self.lineLength] <= 0.001:

                                self.line.length = (
                                    self.line.length + nextLine[self.lineLength] * 1000
                                )
                                self.duplicateElements[
                                    AttachedModule[self.terminalElementID]
                                ] = AttachedModule[self.terminalElementID]
                                if AttachedModule[self.terminalElementID] == 38262:
                                    self.logger.debug("LineV2: adding duplicate -1")
                                ReadLines_V2.trace_line(
                                    self,
                                    conn,
                                    NewTerminals,
                                    self.attachedLineElement[0][self.terminalElementID],
                                    terminalCount,
                                )
                            else:
                                if terminalCount == 1:
                                    if self.transformer == "False" and (
                                        self.filter == "LV" or self.filter == "MV"
                                    ):
                                        elements = self.read_terminal_nodeID(
                                            conn, terminal[self.terminalID]
                                        )
                                        key = False
                                        for Element in elements:
                                            transformers = self.read_twoWindingTransformer(
                                                conn, Element[self.terminalElementID]
                                            )
                                            if len(transformers) > 0:
                                                key = True
                                        if key == True:
                                            self.line.to_element = "sourcebus_" + str(
                                                round(
                                                    self.voltLevel[self.voltageLevelUn]
                                                    * 1000
                                                )
                                            )
                                        else:
                                            self.line.to_element = str(terminal[self.terminalID])
                                    else:
                                        self.line.to_element = str(terminal[self.terminalID])
                                    self.to_name = element[self.elementName]
                                else:
                                    if self.transformer == "False" and (
                                        self.filter == "LV" or self.filter == "MV"
                                    ):
                                        elements = self.read_terminal_nodeID(
                                            conn, terminal[self.terminalID]
                                        )
                                        key = False
                                        for Element in elements:
                                            transformers = self.read_twoWindingTransformer(
                                                conn, Element[self.terminalElementID]
                                            )
                                            if len(transformers) > 0:
                                                key = True
                                        if key == True:
                                            self.line.from_element = "sourcebus_" + str(
                                                round(
                                                    self.voltLevel[self.voltageLevelUn]
                                                    * 1000
                                                )
                                            )
                                        else:
                                            self.line.from_element = str(terminal[self.terminalID])
                                    else:
                                        self.line.from_element = str(terminal[self.terminalID])
                                    self.from_name = element[self.elementName]
                        else:
                            self.terminals.append(terminal[self.terminalID])
                            if terminalCount == 1:
                                if self.transformer == "False" and (
                                    self.filter == "LV" or self.filter == "MV"
                                ):
                                    elements = self.read_terminal_nodeID(
                                        conn, terminal[self.terminalID]
                                    )
                                    key = False
                                    for Element in elements:
                                        transformers = self.read_twoWindingTransformer(
                                            conn, Element[self.terminalElementID]
                                        )
                                        if len(transformers) > 0:
                                            key = True
                                    if key == True:
                                        self.line.to_element = "sourcebus_" + str(
                                            round(
                                                self.voltLevel[self.voltageLevelUn]
                                                * 1000
                                            )
                                        )
                                    else:
                                        self.line.to_element = str(terminal[self.terminalID])
                                else:
                                    self.line.to_element = str(terminal[self.terminalID])
                                self.to_name = element[self.elementName]
                            else:
                                if self.transformer == "False" and (
                                    self.filter == "LV" or self.filter == "MV"
                                ):
                                    elements = self.read_terminal_nodeID(
                                        conn, terminal[self.terminalID]
                                    )
                                    key = False
                                    for Element in elements:
                                        transformers = self.read_twoWindingTransformer(
                                            conn, Element[self.terminalElementID]
                                        )
                                        if len(transformers) > 0:
                                            key = True
                                    if key == True:
                                        self.line.from_element = "sourcebus_" + str(
                                            round(
                                                self.voltLevel[self.voltageLevelUn]
                                                * 1000
                                            )
                                        )
                                    else:
                                        self.line.from_element = str(terminal[self.terminalID])
                                else:
                                    self.line.from_element = str(terminal[self.terminalID])
                                self.from_name = element[self.elementName]

                    if self.from_name == self.to_name:
                        self.line.name = self.from_name.replace(" ", "")
                    else:
                        self.line.name = (
                            self.from_name.replace(" ", "")
                            + "_"
                            + self.to_name.replace(" ", "")
                        )
                    self.logger.debug('Parsed Line named: ' + self.line.name)

    def trace_line(self, conn, terminal, element, terminalCount):
        AttachedModules = self.read_terminal_nodeID(conn, terminal)
        notLine = True
        elementID = ""
        self.attachedLineElement = []
        for AttachedModule in AttachedModules:
            elementID = AttachedModule[self.terminalElementID]
            if (
                element != AttachedModule[self.terminalElementID]
                and len(AttachedModules) == 2
            ):
                attachedLineElements = self.read_lineTerminals(
                    conn, AttachedModule[self.terminalElementID]
                )
                for attachedLineElement in attachedLineElements:
                    if (
                        attachedLineElement[self.terminalNo]
                        != AttachedModule[self.terminalNo]
                    ):
                        self.attachedLineElement.append(attachedLineElement)
                if len(self.attachedLineElement) == 1:
                    line = self.read_line(
                        conn, self.attachedLineElement[0][self.terminalElementID]
                    )
                    if len(line) == 1:
                        notLine = False
                else:
                    notLine = True

        if notLine == False:
            NewTerminals = self.attachedLineElement[0][self.terminalID]
            nextLine = self.read_line(
                conn, self.attachedLineElement[0][self.terminalElementID]
            )[0]
            nextElement = self.read_element(conn, nextLine[self.lineID])[0]
            self.duplicateElements[element] = element
            if element == 38262:
                self.logger.debug("LineV2: adding duplicate 1")
            if (
                self.line.R0 == nextLine[self.lineR0]
                and self.line.R1 == nextLine[self.lineR1]
                and self.line.X0 == nextLine[self.lineX0]
                and self.line.X1 == nextLine[self.lineX1]
            ) or nextLine[self.lineLength] <= 0.001:
                ReadLines_V2.trace_line(
                    self,
                    conn,
                    NewTerminals,
                    self.attachedLineElement[0][self.terminalElementID],
                    terminalCount,
                )
            else:
                if terminalCount == 1:
                    if self.transformer == "False" and (
                        self.filter == "LV" or self.filter == "MV"
                    ):
                        elements = self.read_terminal_nodeID(
                            conn, terminal[self.terminalID]
                        )
                        key = False
                        for Element in elements:
                            transformers = self.read_twoWindingTransformer(
                                conn, Element[self.terminalElementID]
                            )
                            if len(transformers) > 0:
                                key = True
                        if key == True:
                            self.line.to_element = "sourcebus_" + str(
                                round(self.voltLevel[self.voltageLevelUn] * 1000)
                            )
                        else:
                            self.line.to_element = str(terminal[self.terminalID])
                    else:
                        self.line.to_element = str(terminal)
                    self.to_name = nextElement[self.elementName]
                else:
                    if self.transformer == "False" and (
                        self.filter == "LV" or self.filter == "MV"
                    ):
                        elements = self.read_terminal_nodeID(
                            conn, terminal[self.terminalID]
                        )
                        key = False
                        for Element in elements:
                            transformers = self.read_twoWindingTransformer(
                                conn, Element[self.terminalElementID]
                            )
                            if len(transformers) > 0:
                                key = True
                        if key == True:
                            self.line.from_element = "sourcebus_" + str(
                                round(self.voltLevel[self.voltageLevelUn] * 1000)
                            )
                        else:
                            self.line.from_element = str(terminal[self.terminalID])
                    else:
                        self.line.from_element = str(terminal)
                    self.from_name = nextElement[self.elementName]
        else:
            self.duplicateElements[element] = element
            if element == 38262:
                self.logger.debug("LineV2: adding duplicate 2")
            self.terminals.append(terminal)

            nextElement = self.read_element(conn, element)[0]
            if terminalCount == 1:
                if self.transformer == "False" and (
                    self.filter == "LV" or self.filter == "MV"
                ):
                    elements = self.read_terminal_nodeID(conn, terminal)
                    key = False
                    for Element in elements:
                        transformers = self.read_twoWindingTransformer(
                            conn, Element[self.terminalElementID]
                        )
                        if len(transformers) > 0:
                            key = True
                    if key == True:
                        self.line.to_element = "sourcebus_" + str(
                            round(self.voltLevel[self.voltageLevelUn] * 1000)
                        )
                    else:
                        self.line.to_element = str(terminal)
                else:
                    self.line.to_element = str(terminal)
                self.to_name = nextElement[self.elementName]
            else:
                if self.transformer == "False" and (
                    self.filter == "LV" or self.filter == "MV"
                ):
                    elements = self.read_terminal_nodeID(conn, terminal)
                    key = False
                    for Element in elements:
                        transformers = self.read_twoWindingTransformer(
                            conn, Element[self.terminalElementID]
                        )
                        if len(transformers) > 0:
                            key = True
                    if key == True:
                        self.line.from_element = "sourcebus_" + str(
                            round(self.voltLevel[self.voltageLevelUn] * 1000)
                        )
                    else:
                        self.line.from_element = str(terminal)
                else:
                    self.line.from_element = str(terminal)
                self.from_name = nextElement[self.elementName]
        # return
