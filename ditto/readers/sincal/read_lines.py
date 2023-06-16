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


class ReadLines:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_lines(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file

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
            for name in elementColumnNames:
                if name[1] == "Element_ID":
                    self.elementID = name[0]
                elif name[1] == "Type":
                    self.elementType = name[0]
                elif name[1] == "Name":
                    self.elementName = name[0]
                elif name[1] == "Flag_State":
                    self.elementFlagState = name[0]
                elif name[1] == "VoltLevel_ID":
                    self.elementVoltLevel = name[0]
            terminalColumnNames = self.read_terminal_column_names(conn)
            for name in terminalColumnNames:
                if name[1] == "TerminalNo":
                    self.terminalNo = name[0]
                elif name[1] == "Node_ID":
                    self.terminalID = name[0]
                elif name[1] == "Terminal_ID":
                    self.terminalIDNumber = name[0]
                elif name[1] == "Element_ID":
                    self.terminalElementID = name[0]
                elif name[1] == "Flag_Terminal":
                    self.terminalFlagTerminal = name[0]
            lineColumnNames = self.read_line_column_names(conn)
            for name in lineColumnNames:
                if name[1] == "Element_ID":
                    self.lineID = name[0]
                elif name[1] == "Flag_Variant":
                    self.lineFlagVariant = name[0]
                elif name[1] == "l":
                    self.lineLength = name[0]
                elif name[1] == "Flag_LineTyp":
                    self.lineType = name[0]
                elif name[1] == "q":
                    self.lineCrossSection = name[0]
                elif name[1] == "r0":
                    self.lineR0 = name[0]
                elif name[1] == "x0":
                    self.lineX0 = name[0]
                elif name[1] == "r":
                    self.lineR1 = name[0]
                elif name[1] == "x":
                    self.lineX1 = name[0]
                elif name[1] == "c":
                    self.lineC1 = name[0]
                elif name[1] == "c0":
                    self.lineC0 = name[0]
                elif name[1] == "Un":
                    self.lineRatedVoltage = name[0]

            breakerColumnNames = self.read_breaker_column_names(conn)
            for name in breakerColumnNames:
                if name[1] == "Terminal_ID":
                    self.breakerTerminalID = name[0]
                elif name[1] == "Breaker_ID":
                    self.breakerID = name[0]
                elif name[1] == "Flag_Variant":
                    self.breakerFlagVariant = name[0]
                elif name[1] == "Name":
                    self.breakername = name[0]
                elif name[1] == "Flag_Typ":
                    self.breakerType = name[0]
                elif name[1] == "Flag_State":
                    self.breakerState = name[0]

            voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
            for name in voltageLevelColumnNames:
                if name[1] == "Un":
                    self.voltageLevelUn = name[0]

            self.totalLines = 0

            for row in tqdm(Lines, desc='Reading lines', disable=not self.show_progress):
                self.totalLines = self.totalLines + 1
                ReadLines.parse_line(self, row, model,)

        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_line(self, row, model):
        current = self.totalLines
        self.logger.info(f"Thread {__name__} starting")
        # create a database connection
        database = self.input_file
        conn = self.create_connection(database)
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1

        with conn:
            if row[self.lineFlagVariant] == 1:
                element = self.read_element(conn, row[self.lineID])[0]
                voltLevel = self.read_voltageLevel(
                    conn, element[self.elementVoltLevel]
                )[0]
                if voltLevel[self.voltageLevelUn] < voltageLevel:
                    line = Line(model)
                    Terminals = self.read_lineTerminals(conn, row[self.lineID])
                    line.is_enabled = element[self.elementFlagState]
                    for terminal in Terminals:
                        if terminal[self.terminalNo] == 1:
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
                                    line.from_element = "sourcebus_" + str(
                                        round(voltLevel[self.voltageLevelUn] * 1000)
                                    )
                                else:
                                    line.from_element = str(terminal[self.terminalID])
                            else:
                                line.from_element = str(terminal[self.terminalID])
                        elif terminal[self.terminalNo] == 2:
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
                                    line.to_element = "sourcebus_" + str(
                                        round(voltLevel[self.voltageLevelUn] * 1000)
                                    )
                                else:
                                    line.to_element = str(terminal[self.terminalID])
                            else:
                                line.to_element = str(terminal[self.terminalID])
                        line.name = (
                            str(element[self.elementName]).replace(" ", "")
                            + "_"
                            + str(line.to_element)
                            + "_"
                            + str(line.from_element) 
                        )
                        logger.debug(f'Parsed line: {line.name}: {row}')
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
                                    line.is_switch = 1
                                    line.is_enabled = Breaker[0][self.breakerState]
                                    '''switch = Line(model)
                                    switch.name = (
                                        str(Node_ID)
                                        + "_"
                                        + (Breaker[0][self.breakername])
                                        .lower()
                                        .replace(" ", "")
                                    )
                                    switch.is_switch = 1
                                    switch.is_enabled = Breaker[0][self.breakerState]
                                    for breakerTerminal in BreakerTerminals:
                                        if (
                                            breakerTerminal[self.terminalIDNumber]
                                            == terminal[self.terminalIDNumber]
                                        ):
                                            switch.from_element = (
                                                str(terminal[self.terminalID])
                                                + "_"
                                                + str(
                                                    breakerTerminal[
                                                        self.terminalIDNumber
                                                    ]
                                                )
                                            )
                                        else:
                                            switch.to_element = (
                                                str(terminal[self.terminalID])
                                                + "_"
                                                + str(
                                                    breakerTerminal[
                                                        self.terminalIDNumber
                                                    ]
                                                )
                                            )
                                            if terminal[self.terminalNo] == 1:
                                                line.from_element = (
                                                    str(terminal[self.terminalID])
                                                    + "_"
                                                    + str(
                                                        breakerTerminal[
                                                            self.terminalIDNumber
                                                        ]
                                                    )
                                                )
                                            elif terminal[self.terminalNo] == 2:
                                                line.to_element = (
                                                    str(terminal[self.terminalID])
                                                    + "_"
                                                    + str(
                                                        breakerTerminal[
                                                            self.terminalIDNumber
                                                        ]
                                                    )
                                                )
                                    switch.nominal_voltage = row[self.lineRatedVoltage]

                                    switch.length = 0 * 1000
                                    if row[self.lineType] == 1:
                                        switch.line_type = "cable"
                                    elif row[self.lineType] == 2:
                                        switch.line_type = "overhead"
                                    elif row[self.lineType] == 3:
                                        switch.line_type = "connector"
                                    ### Line Phases##################
                                    #
                                    # Phases are given as a string "L1 L2 L3 N"
                                    # Convert this string to a list of characters
                                    #
                                    # phases = terminal[8]
                                    phases = terminal[self.terminalFlagTerminal]
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
                                    # self.logger.debug(sectionPhases)

                                    for phase in sectionPhases:

                                        # Create a Wire DiTTo object
                                        wireBreaker = Wire(model)

                                        # Set the phase
                                        wireBreaker.phase = phase

                                        if line.line_type == "cable":
                                            wireBreaker.nameclass = "Cable_" + line.name
                                        elif line.line_type == "overhead":
                                            wireBreaker.nameclass = "Wire_" + line.name

                                        wireBreaker.gmr = (
                                            math.sqrt(
                                                row[self.lineCrossSection] / math.pi
                                            )
                                            * 1000
                                        )

                                        wireBreaker.diameter = wireBreaker.gmr * 2
                                        # Set the resistance of the conductor
                                        # Represented in Ohms per meter
                                        wireBreaker.resistance = 0
                                        switch.wires.append(wireBreaker)

                                    r0 = 0.00001
                                    r1 = 0.00001
                                    x0 = 0.00001
                                    x1 = 0.00001
                                    switch.R0 = r0
                                    switch.R1 = r1
                                    switch.X0 = x0
                                    switch.X1 = x1

                                    switch.C0 = 0.00001
                                    switch.C1 = 0.00001

                                '''

                        else:
                            Nodes = self.read_lineTerminalsByNodeID(
                                conn, terminal[self.terminalID]
                            )
                            i = 0
                            j = 0
                            for Node in Nodes:
                                i = i + 1
                                Breaker = self.read_breaker(
                                    conn, Node[self.terminalIDNumber]
                                )
                                if len(Breaker) > 0:
                                    if Breaker[0][self.breakerFlagVariant] == 1:
                                        if terminal[self.terminalNo] == 1:
                                            line.from_element = str(terminal[self.terminalID])
                                        elif terminal[self.terminalNo] == 2:
                                            line.to_element = str(terminal[self.terminalID])

                    line.nominal_voltage = row[self.lineRatedVoltage]
                    line.length = row[self.lineLength] * 1000
                    if row[self.lineType] == 1:
                        line.line_type = "cable"
                    elif row[self.lineType] == 2:
                        line.line_type = "overhead"
                    elif row[self.lineType] == 3:
                        line.line_type = "connector"

                    ### Line Phases##################
                    #
                    # Phases are given as a string "L1 L2 L3 N"
                    # Convert this string to a list of characters
                    #
                    # phases = terminal[8]
                    phases = terminal[self.terminalFlagTerminal]

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

                        # Create a Wire DiTTo object
                        wire = Wire(model)

                        # Set the phase
                        wire.phase = phase

                        if line.line_type == "cable":
                            wire.nameclass = "Cable_" + line.name
                        elif line.line_type == "overhead":
                            wire.nameclass = "Wire_" + line.name

                        try:
                            wire.gmr = (
                                math.sqrt(row[self.lineCrossSection] / math.pi) * 1000
                            )
                        except:
                            self.logger.debug("Null cross section area parameter - q")
                            wire.gmr = float('nan')
                        wire.diameter = wire.gmr * 2
                        # Set the resistance of the conductor
                        # Represented in Ohms per meter
                        wire.resistance = (row[self.lineR1] / 1000) * line.length
                        line.wires.append(wire)

                    r0 = row[self.lineR0] / 1000
                    r1 = row[self.lineR1] / 1000
                    x0 = row[self.lineX0] / 1000
                    x1 = row[self.lineX1] / 1000
                    line.R0 = row[self.lineR0]
                    line.R1 = row[self.lineR1]
                    line.X0 = row[self.lineX0]
                    line.X1 = row[self.lineX1]
                    if line.R0 == 0:
                        line.R0 = 0.00001
                    if line.R1 == 0:
                        line.R1 = 0.00001
                    if line.X0 == 0:
                        line.X0 = 0.00001
                    if line.X1 == 0:
                        line.X1 = 0.00001

                    c = row[self.lineC1] / 1000
                    c0 = row[self.lineC0] / 1000
                    if c == 0:
                        c = 0.0001
                    if c0 == 0:
                        c0 = 0.0001
                    line.C1 = c
                    line.C0 = c0
                    try:
                        if self.bus == line.to_element:
                            self.buses.append(line.from_element)
                        else:
                            self.buses.append(line.to_element)
                    except:
                        self.logger.debug(line.to_element)
                    """self.logger.debug("BUSES ")
                    self.logger.debug(line.to_element)
                    self.logger.debug(line.from_element)
                    self.logger.debug(self.buses)"""
            self.logger.debug(f"Thread {__name__} finishing {current}")

    def parse_LV_Lines(self, model, bus):
        self.logger.info(f"Thread {__name__} starting")
        database = self.input_file
        self.key = False
        self.bus = bus
        self.buses = []
        conn = self.create_connection(database)
        with conn:
            elements = self.read_lineTerminalsByNodeID(conn, bus)
            elementColumnNames = self.read_element_column_names(conn)
            for name in elementColumnNames:
                if name[1] == "Element_ID":
                    self.elementID = name[0]
                elif name[1] == "Type":
                    self.elementType = name[0]
                elif name[1] == "Name":
                    self.elementName = name[0]
                elif name[1] == "Flag_State":
                    self.elementFlagState = name[0]
                elif name[1] == "VoltLevel_ID":
                    self.elementVoltLevel = name[0]
            terminalColumnNames = self.read_terminal_column_names(conn)
            for name in terminalColumnNames:
                if name[1] == "TerminalNo":
                    self.terminalNo = name[0]
                elif name[1] == "Node_ID":
                    self.terminalID = name[0]
                elif name[1] == "Terminal_ID":
                    self.terminalIDNumber = name[0]
                elif name[1] == "Element_ID":
                    self.terminalElementID = name[0]
                elif name[1] == "Flag_Terminal":
                    self.terminalFlagTerminal = name[0]
            lineColumnNames = self.read_line_column_names(conn)
            for name in lineColumnNames:
                if name[1] == "Element_ID":
                    self.lineID = name[0]
                elif name[1] == "Flag_Variant":
                    self.lineFlagVariant = name[0]
                elif name[1] == "l":
                    self.lineLength = name[0]
                elif name[1] == "Flag_LineTyp":
                    self.lineType = name[0]
                elif name[1] == "q":
                    self.lineCrossSection = name[0]
                elif name[1] == "r0":
                    self.lineR0 = name[0]
                elif name[1] == "x0":
                    self.lineX0 = name[0]
                elif name[1] == "r":
                    self.lineR1 = name[0]
                elif name[1] == "x":
                    self.lineX1 = name[0]
                elif name[1] == "c":
                    self.lineC1 = name[0]
                elif name[1] == "c0":
                    self.lineC0 = name[0]
                elif name[1] == "Un":
                    self.lineRatedVoltage = name[0]
                elif name[1] == "LineInfo":
                    self.lineInfo = name[0]

            breakerColumnNames = self.read_breaker_column_names(conn)
            for name in breakerColumnNames:
                if name[1] == "Terminal_ID":
                    self.breakerTerminalID = name[0]
                elif name[1] == "Breaker_ID":
                    self.breakerID = name[0]
                elif name[1] == "Flag_Variant":
                    self.breakerFlagVariant = name[0]
                elif name[1] == "Name":
                    self.breakername = name[0]
                elif name[1] == "Flag_Typ":
                    self.breakerType = name[0]
                elif name[1] == "Flag_State":
                    self.breakerState = name[0]

            voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
            for name in voltageLevelColumnNames:
                if name[1] == "Un":
                    self.voltageLevelUn = name[0]

            self.totalLines = 0
            threads = list()

            for element in elements:
                try:
                    if not element[self.terminalElementID] in self.usedLines:
                        row = self.read_line(conn, element[self.terminalElementID])
                        if len(row)>0:
                            row = row[0]
                            self.totalLines = self.totalLines + 1
                            ReadLines.parse_line(self, row, model)
                            self.usedLines[element[self.terminalElementID]] = element[
                                self.terminalElementID
                            ]
                        else:
                            self.logger.debug("Not a line", exc_info=True)
                except:
                    self.logger.warning("Error while reading Line", exc_info=True)
        return self.buses