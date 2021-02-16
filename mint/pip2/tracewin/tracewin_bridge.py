import os

class TraceWinProcess:
    def __init__(self):
        """
        Initializes the parameters for the TraceWin optimization
        """
        self.parent_folder = os.path.dirname(os.path.abspath(__file__))

        # Modify these files for use in the other applications.
        self.dat_file = self.parent_folder + "/TW_PIP2IT_PAVLO_08032020/PIP2IT_Feb2019.dat"
        self.tracewin_script_loc = self.parent_folder + '/TW_PIP2IT_PAVLO_08032020/TraceWin PIP2IT_Feb2019.ini'
        self.partran_output = self.parent_folder + "/TW_PIP2IT_PAVLO_08032020/partran1.out"

    def simulate(self):
        '''
        Runs the TraceWin with the new parameters
        '''

        # Runs TraceWin script based on the optimization parameters.
        os.system(self.tracewin_script_loc)

    def get_input_parameters(self, coordinate_map):
        """
        Obtains the parameters to optimize via the coordinate map
        :param coordinate_map: Map of coordinates {line#: [position1, position2,...], ....}
        :return value map dictionary {"line,position1": value_extracted1, "line,position2": value_extracted2, ...}
        """
        f = open(self.dat_file, "r")
        i = 0
        value_map = dict()
        for x in f:  # Iterate through every single line until we reach a value in our coordinate map
            i+=1
            if i in coordinate_map:
                device_properties = x.strip().split()  # Line split by spaces
                for j in coordinate_map[i]:
                    value_map["{},{}".format(i, j)] = float(device_properties[j])  # Gets the space-seperated value and  places in list
        f.close()
        return value_map

    def modify_params(self, value_map):
        """
        Updates the dat file with new parameters.
        :param value_map: Dictionary, key is of the form "line,position" where line and position are placeholders,
        value corresponds to the target value (e.g key="215,2" would correspond to the 215 line, 2nd space separated position
        in the dat file"
        :return No return
        """

        # TODO: Fix this up better
        f = open(self.dat_file, "r")
        g = open(self.dat_file+".tmp", "w")
        i = 0

        line_lookup = self._fast_lookup_representation(value_map)

        for x in f:
            i+=1
            if i in line_lookup:
                temporary_line_split = x.strip().split()
                for ssv, value in line_lookup[i]:
                    temporary_line_split[ssv] = str(value)
                updated_line = " ".join(temporary_line_split) + '\n'
                g.write(updated_line)
            else:
                g.write(x)
        f.close()
        g.close()

        # Renames the file for continous optimization
        os.rename(self.dat_file+".tmp", self.dat_file)

    def get_output_values(self):
        """
        Runs and extracts values form the Partran.out file.
        """
        parameter_names = []
        parameter_values = []
        f = open(self.partran_output, "r")
        for x in f:
            if x.startswith(" ##"):
                parameter_names = x.split()[1:]
            parameter_values = x.split()[1:]
        parameter_values = [float(val) for val in parameter_values]
        return dict(zip(parameter_names, parameter_values))

    def get_loss_function(self, loss_param):
        """
        Gets the loss function based from the output parameters.
        @param loss_param: the keyword for the loss parameter
        """

        values = self.get_output_values()
        return values[loss_param]

    def covert_coordinate_map_to_string(self, coordinate_map):
        """
        Converts the Coordinate map dictionary into a usable coordinate map
        @param coordinate_map: dictionary key value pair
        """

        coordinate_list = []

        for line, entry in coordinate_map.items():
            coordinate_list.append(self.coordinate_to_string(line, entry))

        return coordinate_list

    def coordinate_to_string(self, line, entry):
        """
        Converts a line, entry pair (x,y) to the string representation "x,y"
        @param line: line number in the DAT file
        @param entry: entry separated by spaces in the line above.
        @return String "x,y"
        """

        return str(line)+","+str(entry)

    def _fast_lookup_representation(self, update_map):
        """
        Gets the fast lookup representation for use in parameter updates.
        """

        fast_dict = {}

        for key, value in update_map.items():
            line_ssv = key.split(",") # ssv = space seperated value
            line = int(line_ssv[0])
            ssv = int(line_ssv[1])
            if line not in fast_dict:
                fast_dict[line] = [(ssv, value)]
            else:
                fast_dict[line].append((ssv, value))

        return fast_dict



if __name__ == "__main__":
    tw = TraceWinProcess()
    tw.simulate()
    print(tw.get_output_values())