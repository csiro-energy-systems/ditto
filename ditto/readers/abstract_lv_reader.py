# Created by Sam West (sam.west@csiro.au) at 4/02/2021
import logging
from abc import abstractmethod

import numpy as np
from tqdm import tqdm

from ditto import Store
from ditto.models import PowerTransformer
from ditto.models.power_source import PowerSource
from ditto.readers.abstract_reader import AbstractReader

logger = logging.getLogger(__name__)


class AbstractLVReader(AbstractReader):
    """
    An Abstract reader that is capable of splitting a larger network into separate LV networks, for readers that implement the various get_LV*() methods.
    """

    @abstractmethod
    def get_LV_Transformers(self, model):
        pass

    @abstractmethod
    def get_lv_transformers(self, model, lv_busname):
        pass

    @abstractmethod
    def get_lv_loads(self, model, lv_busname):
        pass

    @abstractmethod
    def get_lv_photovoltaics(self, model, lv_busname):
        pass

    @abstractmethod
    def get_lv_lines(self, model, lv_busname):
        pass

    @abstractmethod
    def get_lv_node(self, model, lv_busname):
        pass

    def parse_lv_networks(self, m: Store, show_progress: bool, **kwargs) -> list:
        """
        Parses all transformers first, then creates a list of submodels from all elements on the lower-voltage side of each.
        Note: After parsing self.model will just be the entire network (should be the same as if parsed by parse_whole_network()), but the function returns a list of sub-models.

        :param m: the DiTTo Store object to parse
        :param show_progress: whether to show a progress bar
        :return: a list of downstream-network-models in ditto Store format.
        """

        self.get_lv_transformers(m)

        # Loop over the DiTTo objects
        self.usednetworkbuses = {}
        lv_networks = []

        transformers = [m for m in m.models if isinstance(m, PowerTransformer)]

        for trans in tqdm(transformers, desc=f'Reading network downstream of LV transformers from {self.input_file}', disable=not show_progress):
            # logger.debug(f'Reading LV network from Transformer "{trans.name}"')

            lv_network = self.parse_lv_network_from(trans)

            ''' Make sure we're not duplicating any existing network names. '''
            dup_count = sum([l.name == lv_network.name for l in lv_networks])
            loop_count = 2
            while dup_count > 0:  # if there are any duplicates in the list already
                new_name = lv_network.name.replace(f' #{loop_count - 1}', '') + f' #{loop_count}'  # drop the previously used ' #n' suffix and append the new one ' #n+1
                self.logger.warning(f'Duplicate network name "{lv_network.name}" found, renaming to "{new_name}"')
                lv_network.name = new_name
                dup_count = sum([l.name == lv_network.name for l in lv_networks])
                loop_count += 1

            # Make sure the model_names dict is up to date
            lv_network.set_names()

            ''' Experimental: remove any links that create loops and look like switches/fuses etc '''
            # if remove_cycles:
            #     for obj in lv_network.models:
            #         if isinstance(obj, PowerSource) and obj.is_sourcebus == 1:
            #             power_source_names.append(obj.name)
            #
            #     power_source_names = np.unique(power_source_names)
            #     decycled_networks = open_switches_in_cycles(lv_network, power_source_names[0], line_unique_features=['R1', 'X1', 'R0', 'X0', 'line_type', 'nominal_voltage', 'nameclass'])
            #     lv_networks.extend(decycled_networks)

            lv_networks.append(lv_network)

        return lv_networks

    def parse_lv_network_from(self, trans):

        lv_network = Store()
        self.logger.info('Tracing LV lines from transformer: ' + trans.name)

        LVBus = ""
        if trans.windings[0].nominal_voltage < trans.windings[1].nominal_voltage:
            LVBus = trans.from_element
            sourcebus = trans.windings[1].nominal_voltage
        else:
            LVBus = trans.to_element
            sourcebus = trans.windings[0].nominal_voltage
        self.get_LV_Transformer(lv_network, LVBus)
        self.get_lv_loads(lv_network, LVBus)
        self.get_lv_photovoltaics(lv_network, LVBus)

        # FIXME: This test breaks the inheritance, find a way to improve if/when incorporating the PowerFactory reader
        # if type(self) == ditto.readers.powerfactory.read.Reader:
        #     usedfeederbuses = self.get_LV_Lines(lv_network, LVBus, self.usednetworkbuses)
        #     self.usednetworkbuses.update(usedfeederbuses)
        # else:
        #     self.get_LV_Lines(lv_network, LVBus)

        self.get_lv_lines(lv_network, LVBus)
        self.get_lv_node(lv_network, LVBus)

        ''' Make a node for the sourcebus '''
        source = PowerSource(lv_network)
        source.name = trans.from_element
        source.nominal_voltage = sourcebus
        source.phases.append("A")
        source.phases.append("B")
        source.phases.append("C")
        source.is_sourcebus = True
        source.connection_type = "Y"
        # source.connecting_element = trans.name

        lv_network.name = source.name + '.' + trans.name
        lv_network.bus_name = LVBus
        # lv_network.input_file = self.input_file

        ''' 
        Check for duplicate models names.  This shouldn't ever (I think) happen, and probably indicates a problem with the parser or source model data.
        We can't fix duplicated here because we don't know which edges should actually connect to which nodes now, so have to just print an error and hope someone notices! 
        '''
        import collections
        name_counts = collections.Counter([m.name for m in lv_network.models if hasattr(m, 'name')])
        dup_names = {name: name_counts[name] for name in name_counts.keys() if name_counts[name] > 1}
        if len(dup_names) > 0:
            self.logger.error(f'In network "{lv_network.name}" - found and renamed {len(dup_names)} duplicate named models : {dup_names}.')

        lv_network.set_names()
        return lv_network


def get_impedance_from_matrix(impedance_matrix):
    '''
    Gets lines impedances in ditto's format (R0, R1, X0, X1) a 3x3 impedance matrix in 'Kron reduced format'.
    This essentially solves Equation 13 from:
        W. H. Kersting and W. H. Phillips, "Distribution feeder line models," Proceedings of 1994 IEEE Rural Electric Power Conference, Colorado Springs, CO, USA, 1994, pp. A4/1-A4/8,
        doi: 10.1109/REPCON.1994.326257
    where `impedance-matrix` is `Zabc` and Z00=R0+X0j and Z11=R1+X1j are the first two diagonals from Z012.
    So we just solve [ZO12] = [A]^-1 [Zabc] [A] to get Z012, and pluck out the first two diagonal elements.
    :param impedance_matrix: Zabc from the paper (equivalent to ditto.models.Line.impedance_matrix)
    :return: R0, X0, R1. X1
    '''

    if np.shape(impedance_matrix) != (3, 3):
        raise ArithmeticError(f'Invalid impedance matrix found : {impedance_matrix}')

    from numpy import exp, pi, matrix, real, imag
    from numpy.linalg import inv
    alpha = exp((2 * pi) / 3j)
    a = matrix(
        [[1, 1, 1],
         [1, alpha ** 2, alpha],
         [1, alpha, alpha ** 2]])

    # impedance_matrix = matrix[[1, 2, 3], [4, 5, 6], [7, 8, 9]] #for testing
    Zabc = impedance_matrix
    Z012 = a * Zabc * inv(a)

    Z00 = Z012[0, 0]
    Z11 = Z012[1, 1]
    R0 = round(real(Z00), 3)
    X0 = round(imag(Z00), 3)
    R1 = round(real(Z11), 3)
    X1 = round(imag(Z11), 3)
    return R0, X0, R1, X1
