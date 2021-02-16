from mint.opt_objects import MachineInterface
import os
from mint.pip2.acsys import DPM

"""
Machine Interface for PIP-II
"""


class PIPIIMachineInterface(MachineInterface):
    name = "PIPIIMachineInterface"

    def __init__(self, args):
        super(PIPIIMachineInterface, self).__init__(args)
        self.config_dir = os.path.join(self.config_dir,
                                       "pip2")  # <ocelot>/parameters/
        self._save_at_exit = False
        self._use_num_points = False ## TODO: what does this mean
        self.read_only = False

        # Data for the values
        self.pvs = dict()


    def get_value(self, device):
        '''
        Gets the value from a device
        '''

        with DPM.Blocking() as dpm:
            dpm.add_entry(0, device)
            for event_response in dpm.process():
                self.pvs[device] = event_response
                break
            return None


    def set_value(self, channel, val):
        self.pvs[channel] = val # updates the value in the dictionary

        # TODO: send a value to the device in accelerator setup.