import logging

from ditto.models.power_source import PowerSource
from ditto.readers.sincal.exception_logger import log_exceptions

logger = logging.getLogger(__name__)


class ReadSources:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_sources(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        conn = self.get_conn()

        Infeeders = self.read_infeeders(conn)
        elementColumnNames = self.read_element_column_names(conn)
        for idx, name in enumerate(elementColumnNames):
            if name["name"] == "Element_ID":
                self.elementID = idx
            elif name["name"] == "Flag_State":
                self.elementFlagState = idx
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

        infeederColumnNames = self.read_infeeder_column_names(conn)
        for idx, name in enumerate(infeederColumnNames):
            if name["name"] == "Element_ID":
                self.infeederID = idx
            elif name["name"] == "delta":
                self.infeederDeta = idx
            elif name["name"] == "Flag_Variant":
                self.infeederFlagVariant = idx

        voltageLevelColumnNames = self.read_voltageLevel_column_names(conn)
        for idx, name in enumerate(voltageLevelColumnNames):
            if name["name"] == "Un":
                self.voltageLevelUn = idx

        synchronousMachines = self.read_synchronousMachines(conn)
        synchronousMachineColumnNames = self.read_synchronousMachines_column_names(conn)
        for idx, name in enumerate(synchronousMachineColumnNames):
            if name["name"] == "Flag_Variant":
                self.synchronousMachineFlagVariant = idx

        from tqdm import tqdm

        self.totalSynchronousMachines = 0

        for synchronousMachine in tqdm(
            synchronousMachines,
            desc="Reading synchronous machines",
            disable=not self.show_progress,
        ):
            self.totalSynchronousMachines = self.totalSynchronousMachines + 1
            ReadSources.parse_synchronousMachine(self, synchronousMachine, model)

        self.totalInfeeders = 0
        for infeeder in tqdm(
            Infeeders, desc="Reading infeeders", disable=not self.show_progress
        ):
            self.totalInfeeders = self.totalInfeeders + 1
            ReadSources.parse_infeeders(self, infeeder, model)

        ReadSources.set_infeeders(self, model)
        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_synchronousMachine(self, synchronousMachine, model):
        current = self.totalSynchronousMachines
        self.logger.debug(
            f"Thread {__name__} starting %s", self.totalSynchronousMachines
        )
        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        if self.filter == "LV":
            voltageLevel = 1

        if synchronousMachine[self.synchronousMachineFlagVariant] == 1:
            element = self.read_element(conn, synchronousMachine[self.infeederID])[0]
            if element[self.elementFlagState] == 1:
                voltLevel = self.read_voltageLevel(
                    conn, element[self.elementVoltLevel]
                )[0]
                if voltLevel[self.voltageLevelUn] < voltageLevel:
                    terminal = self.read_terminal(conn, element[self.infeederID])[0]
                    source = PowerSource(model)
                    source.name = str(element[self.elementName])
                    self.logger.debug("Source name: " + source.name)
                    source.nominal_voltage = voltLevel[self.voltageLevelUn] * 10**3
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
        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1

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
                    self.logger.debug("Source name: " + source.name)
                    # Set the nominal voltage
                    source.nominal_voltage = (
                        voltLevel[self.voltageLevelUn] * 10**3
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
                self.logger.debug("Source name: " + source.name)
                # Set the nominal voltage
                source.nominal_voltage = (
                    voltlevel[self.voltageLevelUn] * 10**3
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
        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1

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
                    self.logger.debug("Source Name: " + source.name)
                    source.nominal_voltage = voltlevel[self.voltageLevelUn] * 10**3
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
                    self.logger.debug("Source Name: " + source.name)
                    source.nominal_voltage = voltlevel[self.voltageLevelUn] * 10**3
                    source.phases.append("A")
                    source.phases.append("B")
                    source.phases.append("C")
                    source.is_sourcebus = True
                    source.connection_type = "Y"
                    source.connecting_element = "sourcebus_" + str(
                        round(voltlevel[self.voltageLevelUn] * 1000)
                    )
