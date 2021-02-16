from mint.opt_objects import MachineInterface
import os
import mint.pip2.tracewin.tracewin_bridge as twbridge

"""
Machine Interface for PIP-II
"""


class PIPIISimulatorInterface(MachineInterface):
    name = "PIPIISimulatorInterface"

    def __init__(self, args):
        super(PIPIISimulatorInterface, self).__init__(args)
        self.config_dir = os.path.join(self.config_dir,
                                       "pip2/simulator")  # <ocelot>/parameters/
        self._save_at_exit = False
        self._use_num_points = False ## TODO: what does this mean
        self.read_only = False

        self.tw = twbridge.TraceWinProcess()

        # Data for the values
        self.pvs = dict()

        # Optimization input coordinate map
        self.coordinate_map = {13: [1], 14: [1], 15: [1]}

        # Loss function would come from the patran output file.
        self.target_parameter = "W0"


    def get_value(self, device):
        '''
        Gets the value from a device
        '''

        if device == self.target_parameter:
            self.pvs[device] = self.tw.get_loss_function(self.target_parameter)
        else:
            line_val = self.tw.coordinate_to_string(device, self.coordinate_map[device])
            self.pvs[line_val] = self.tw.get_input_parameters(self.coordinate_map)[device]
        return self.pvs[device]


    def set_value(self, device, val):
        """
        Sets the device with a given value
        """

        self.pvs[device] = val # updates the value in the dictionary
        if device == self.target_parameter:
            self.tw.simulate()
        self.tw.modify_params({device: val})