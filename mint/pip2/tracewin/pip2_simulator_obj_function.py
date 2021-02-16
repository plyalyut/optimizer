from mint.opt_objects import Target
from mint.pip2.tracewin.pip2_simulator_interface import PIPIISimulatorInterface
import time


class TracewinTarget(Target):
    def __init__(self, mi=None, eid = None):
        # TODO: setup the eid
        super(TracewinTarget, self).__init__(eid=eid)

        self.mi = mi
        self.kill = False
        self.objective_acquisition = None
        self.points = 1

    def get_value(self):
        """
        Gets the value of an optimization function
        """

        print('Getting value from optimization function')
        self.objective_acquisition = self.mi.get_value(self.eid)
        return self.objective_acquisition

    def get_penalty(self):
        """
        Performs the penalty optimization on the script.
        """

        # TODO: figure out what this is referring to
        sase = self.get_value()
        alarm = self.get_alarm()
        pen = 0.0
        if alarm > 1.0:
            return self.pen_max
        if alarm > 0.7:
            return alarm * 50.0

        self.penalties.append(pen)
        self.times.append(time.time())
        self.values.append(sase)
        self.objective_acquisitions.append(self.objective_acquisition)
        self.std_dev.append(0)
        self.alarms.append(alarm)
        self.niter += 1
        return pen

    def clean(self):
        """
        Cleans the optimization function for next iterations.
        """
        Target.clean(self)
        self.objective_acquisitions = []  # all the points
        self.objective_means = []
        self.std_dev = []
        self.charge = []
        self.current = []
        self.losses = []
