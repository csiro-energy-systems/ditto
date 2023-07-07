import logging
import math

from ditto.models.reactor import Reactor
from ditto.models.phase_reactor import PhaseReactor
from ditto.readers.sincal.exception_logger import log_exceptions

logger = logging.getLogger(__name__)


class ReadReactors:
    logger = logging.getLogger(__name__)

    @log_exceptions
    def parse_reactors(self, model, show_progress=True):
        self.show_progress = show_progress
        self.logger.info(f"Thread {__name__} starting")
        conn = self.get_conn()

        Elements = self.read_elements(conn)

        self.totalReactors = 0

        from tqdm import tqdm

        for element in tqdm(
            Elements, desc="Reading reactors", disable=not self.show_progress
        ):
            self.totalReactors = self.totalReactors + 1
            ReadReactors.parse_reactor(self, element, model)

        self.logger.debug(f"Thread {__name__} finishing")

    @log_exceptions
    def parse_reactor(self, element, model):
        current = self.totalReactors
        self.logger.debug(f"Thread {__name__} starting %s", self.totalReactors)

        conn = self.get_conn()
        voltageLevel = 99999999
        if self.filter == "MV":
            voltageLevel = 35
        elif self.filter == "LV":
            voltageLevel = 1

        if element[3] == "SerialReactor" and element[2] == 1:
            self.logger.debug("Start Serial Reactor")
            # serialReactor = self.read_serialReactor(conn, element[0])[0]
            voltLevel = self.read_voltageLevel(conn, element[11])[0]
            if voltLevel[self.voltageLevelUn] < voltageLevel:
                reactor = Reactor(model)
                terminal = self.read_terminal(conn, element[0])
                # Set the name
                reactor.name = element[8].replace(" ", "").lower()
                self.logger.debug("Reactor Name: " + reactor.name)
                # Set the connecting element
                reactor.from_element = str(terminal[0][4])
                reactor.to_element = str(terminal[0][4])

                phase = terminal[0][8]
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
                # Set the nominal voltage
                # Convert from KV to Volts since DiTTo is in volts
                reactor.nominal_voltage = voltLevel[6] * 10**3  # DiTTo in volts
                if len(phases) == 3:
                    reactor.nominal_voltage * math.sqrt(3)

                # For each phase...
                for p in phases:
                    phaseReactor = PhaseReactor(model)
                    phaseReactor.phase = p
                    reactor.phase_reactors.append(phaseReactor)

            elif element[3] == "ShuntReactor" and element[2] == 1:
                self.logger.debug("Start Shunt Reactor")
                reactor = Reactor(model)
                shuntReactor = self.read_shuntReactor(conn, element[0])[0]
                voltLevel = self.read_voltageLevel(conn, element[11])[0]
                terminal = self.read_terminal(conn, element[0])
                # Set the name
                reactor.name = element[8].replace(" ", "_").lower()
                self.logger.debug("Reactor Name: " + reactor.name)
                # Set the connecting element
                reactor.from_element = str(terminal[0][4])
                reactor.to_element = str(terminal[0][4])

                phase = terminal[0][8]
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
                # Set the nominal voltage
                # Convert from KV to Volts since DiTTo is in volts
                reactor.nominal_voltage = voltLevel[6] * 10**3  # DiTTo in volts
                if len(phases) == 3:
                    reactor.nominal_voltage * math.sqrt(3)

                # For each phase...
                for p in phases:
                    phaseReactor = PhaseReactor(model)
                    phaseReactor.phase = p
                    reactor.phase_reactors.append(phaseReactor)
                    if p == "A":
                        phaseReactor.rated_power = (
                            float(shuntReactor[7]) * 10**6
                        )  # Ditto in var
                    if p == "B":
                        phaseReactor.rated_power = (
                            float(shuntReactor[7]) * 10**6
                        )  # Ditto in var
                    if p == "C":
                        phaseReactor.rated_power = (
                            float(shuntReactor[7]) * 10**6
                        )  # Ditto in var
                    # self.logger.debug(phaseReactor.rated_power)
                    reactor.phase_reactors.append(phaseReactor)
        self.logger.debug(f"Thread {__name__} finishing %s", current)
