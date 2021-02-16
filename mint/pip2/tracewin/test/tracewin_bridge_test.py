from unittest import TestCase
from mint.pip2.tracewin.tracewin_bridge import TraceWinProcess

class TraceWinBridgeTest(TestCase):
    """
    Unit Test Suite for the TraceWin Bridge
    """

    def tracewin_bridge(self):
        self.tw = TraceWinProcess()

    def test_run(self):
        """
        Test the simulation procedure.
        """
        self.tracewin_bridge()
        self.tw.simulate()

    def test_read(self):
        """
        Gets the values after the simulation
        """
        self.tracewin_bridge()
        extracted_values = self.tw.get_output_values()
        self.assertTrue(len(extracted_values) > 0)
        self.assertTrue('W0' in extracted_values)

    def test_obtain_optimization_parameters(self):
        """
        Gets the optimization parameters from a given line, test.
        """
        self.tracewin_bridge()
        coordinates_values_to_get = {219: [1, 2]}
        value_map = self.tw.get_input_parameters(coordinates_values_to_get)
        self.assertEqual(value_map['219,1'], 50.0)
        self.assertEqual(value_map['219,2'], 15)

    def test_create_new_partran(self):
        """
        Test creating new Partran file.
        """
        self.tracewin_bridge()
        self.tw.modify_params({"219,1": 201, "219,2": 15.01})