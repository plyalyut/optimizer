"""This module provides access to the ACNET Control System's Data Pool
Manager (DPM) allowing Python scripts to efficiently obtain ACNET
device data.

Example: This simple example prints the outdoor temperature (via
M:OUTTMP) once a second. A more serious implementation would handle
errors.

    import DPM

    dpm = DPM.Connection()
    dpm.add_entry(0, 'M:OUTTMP@p,1000')
    dpm.start()

    for ii in dpm.process():
        if isinstance(ii, DPM.ItemData):
            print 'Temperature: ' + str(ii.data) + ' F'

NOTE: This is a new and untested module! Although we know we can
retrieve accelerator data with this interface, there may be unexpected
behavior lingering in the code:

  - If a list is restarted (using 'start()'), there may be ItemData
    objects that still need to be read from the incoming stream.

"""

import time
import logging
import mint.pip2.acsys.dpm_protocol as dpm_protocol
import mint.pip2.acnetd.acnet as acnet
import sys


class ItemData:
    """An object that holds a value from a device.

    DPM delivers device data using a stream of ItemData objects. The
    'tag' field corresponds to the tag parameter used when the
    'addDevice' method was used to add the device to the list.

    The 'stamp' field is the timestamp when the data occurred.

    The 'data' field is the requested data. The data will be of the
    type asked in the corresponding DRF2 (specified in the call to the
    'addRequest' method.) For instance, if .RAW was specified, the
    'data' field will contain a bytearray(). Otherwise it will contain
    a scaled, floating point value (or an array, if it's an array
    device.)

    """

    def __init__(self, tag, stamp, cycle, data, micros=None):
        self.tag = tag
        self.stamp = float(stamp) / 1000.0
        self.data = data
        self.cycle = cycle
        if micros is not None:
            self.micros = micros

    def __str__(self):
        guaranteed_fields = "{tag: " + str(self.tag) + ", stamp: " + str(
            self.stamp) + ", data: " + str(self.data)
        if hasattr(self, 'micros'):
            return guaranteed_fields + ", micros: " + str(self.micros) + "}"

        return guaranteed_fields + "}"


class ItemStatus:
    """An object reporting status of an item in a DPM list.

    If there was an error in a request, this object will be in the
    stream instead of a ItemData object. The 'tag' field corresponds
    to the tag parameter used in the call to the 'addRequest' method.

    The 'status' field describes the error that occurred with this
    item.

    If this message appears, there will never be an ItemData object
    for the 'tag' until the error condition is fixed and the list
    restarted.

    """

    def __init__(self, tag, status):
        self.tag = tag
        self.status = status

    def __str__(self):
        return ("{tag: " + str(self.tag) + ", status: " + str(self.status) +
                "}")

# This private function lets us do a request/reply using messages
# defined in the dpm_protocol module.


def _rpc(con, task, msg):
    try:
        pkt = bytearray(msg.marshal())
        (address, reply) = con.request_single(task, pkt, 500)
        return (address, dpm_protocol.unmarshal_reply(iter(reply)))
    except dpm_protocol.ProtocolError as error:
        logging.warning('error unmarshalling reply: %s', error)
        raise acnet.Status.ACNET_RPLYPACK


class Connection:
    """Manages a connection to an ACNET Data Pool Manager

    This is the base class used by the two connection classes, Blocked
    and Polled. Through this connection, a script can access ACNET
    Control System data. This class implements Python's context
    manager protocol, so it can be used in a with-statement.
    """

    # Constructor. Creates a new instance of a dpm.Connection
    # object. It takes an optional parameter which specifies an ACNET
    # connection object it should use for its communication.

    def __init__(self, con=None, task_node=None):
        """Class constructor. Takes an optional argument indicating an ACNET
        connection to use. If no parameter is passed, the object will
        create its own ACNET connection object to use.
        """

        # We're using the two lower bits of the tag field to help
        # ignore data from previous StartList commands. 'next_count' is
        # used when adding devices and 'curr_count' is used when
        # receiving replies.

        self.settings_enabled = False
        self.next_count = 0
        self.curr_count = 3
        self.dev_list = []
        self.list_id = None
        self.replies = None
        self.dpm_task = None
        if task_node is not None:
            self.task_node = task_node

        # Create the ACNET connection (if necessary) and open a DPM
        # list.

        self.con = acnet.Connection() if con is None else con
        self._connect_to_dpm()

    def __enter__(self):
        return self

    def __exit__(self, ext, exv, trb):
        self.stop()
        return False

    # Helper method to connect to DPM. When this method returns, a DPM
    # will have been selected, a list opened, and any specified
    # devices will have been loaded. Whether or not the list needs to
    # be started depends upon the caller.

    def _connect_to_dpm(self):
        assert (self.replies is None and
                self.list_id is None and
                self.dpm_task is None)

        if hasattr(self, 'task_node'):
            self.dpm_task = self.task_node if '@' in self.task_node else 'DPMJ@' + self.task_node
        else:
            self._service_discovery()

        try:
            self._alt_open_list()
        except:
            del self.dpm_task
            raise

    # A private method to find an available DPM. This method will not
    # return until a DPM is found. XXX: Is this too restrictive?

    def _service_discovery(self):
        assert self.dpm_task is None

        msg = dpm_protocol.ServiceDiscovery_request()

        while True:
            logging.info('looking for an available DPM')
            (_, reply) = _rpc(self.con, 'DPMJ@MCAST', msg)

            if isinstance(reply, dpm_protocol.ServiceDiscovery_reply):
                self.dpm_task = 'DPMJ@' + reply.serviceLocation
                return

            logging.warning('bad discovery reply')
            time.sleep(5.0)

    def _open_list(self, block):
        assert self.dpm_task is not None
        assert self.list_id is None
        assert self.replies is None

        msg = dpm_protocol.OpenList_request()
        pkt = bytearray(msg.marshal())
        gen = self.con.request_multiple(self.dpm_task, pkt, 6000)

        # The first reply from an OpenList request is the OpenList
        # reply which contains the list ID that we need to use
        # everywhere else.

        (_, msg) = gen.next()

        try:
            reply = dpm_protocol.unmarshal_reply(iter(msg))

            assert isinstance(reply, dpm_protocol.OpenList_reply)

            # Set the blocking mode of the generator (it defaults to
            # True, which lets us wait for the first reply.)

            gen.blocking = block

            # Everything is good. Create attributes indicating the
            # updated state of the connection.

            self.replies = gen
            self.list_id = reply.list_id
        except dpm_protocol.ProtocolError as error:
            logging.warning('error unmarshalling reply: %s', error)
            raise acnet.Status.ACNET_RPLYPACK

    # Clears the remote list.

    def _clear_remote(self):
        msg = dpm_protocol.ClearList_request()
        msg.list_id = self.list_id

        (_, reply) = _rpc(self.con, self.dpm_task, msg)

        assert isinstance(reply, dpm_protocol.ListStatus_reply)

    # Adds a tag/DRF2 entry to the remote list.

    def _add_remote(self, tag, drf2):
        msg = dpm_protocol.AddToList_request()
        msg.list_id = self.list_id
        msg.ref_id = int(tag) * 4 + self.next_count
        msg.drf_request = drf2

        (_, reply) = _rpc(self.con, self.dpm_task, msg)

        assert isinstance(reply, dpm_protocol.AddToList_reply)

        sts = acnet.Status(reply.status)
        if sts.is_fatal():
            raise sts

    def _build_struct(self, index, value):
        if not isinstance(value, list) and not isinstance(value, bytearray):
            value = [value]

        if isinstance(value, bytearray):
            set_struct = dpm_protocol.RawSetting_struct()
        elif isinstance(value[0], str):
            set_struct = dpm_protocol.TextSetting_struct()
        else:
            set_struct = dpm_protocol.ScaledSetting_struct()

        (ref_id, _) = self.dev_list[index]
        set_struct.ref_id = ref_id * 4 + self.curr_count

        set_struct.data = value
        return set_struct

    # Clears the list of devices.

    def clear_list(self):
        """Removes all entries from the list.

        If a completely different list of devices is desired, this
        method can be used to quickly clear the list before adding new
        entries. This doesn't actually stop the previous data
        acquistion, it just modifies the list's contents.

        """
        self.dev_list = []

    # Adds a DRF2 specification to the list.

    def add_entry(self, tag, drf2):
        """Add an entry to the list of devices to be acquired.

        This updates the list of device requests. The 'tag' parameter
        is used to mark this request's device data. When the script
        starts receiving ItemData objects, it can correlate the data
        using the 'tag' field.

        If this method is called with a tag that was previously used,
        it replaces the previous request. If data is currently being
        returned, it won't reflect the new entry until the 'start'
        method is called.
        """
        self.dev_list = [(t, d) for (t, d) in self.dev_list
                         if t != tag] + [(tag, drf2)]

    def remote_entry(self, tag):
        """A placeholder for remote entry docstring
        """
        self.dev_list = [(t, d) for (t, d) in self.dev_list if t != tag]

    def apply_settings(self, input_array):
        """A placeholder for apply setting docstring
        """

        if 'gssapi' not in sys.modules:
            import gssapi

        if not hasattr(self, 'creds'):
            try:
                self.creds = gssapi.creds.Credentials(usage='initiate')
                principal = str(self.creds.name).split('@')
                self.user_name = principal[0]
                self.domain = principal[1]
                self.settings_enabled = True
            except:
                logging.info('Error getting Kerberos ticket.')
                delattr(self, 'creds')
                return

        if self.settings_enabled:
            try:
                if self.domain != 'FNAL.GOV' or self.creds.lifetime <= 0:
                    self.settings_enabled = False
                    return
            except:
                self.settings_enabled = False
                logging.warning('No valid Kerberos ticket found.')
                logging.info('Stopping settings until ticket is found.')
                delattr(self, 'creds')
                return

            if not isinstance(input_array, list):
                input_array = [input_array]

            msg = dpm_protocol.ApplySettings_request()
            msg.list_id = self.list_id
            msg.user_name = self.user_name

            all_settings = []

            for (index, input_val) in enumerate(input_array):
                if isinstance(input_val, tuple):
                    all_settings.append(self._build_struct(
                        input_val[0], input_val[1]))
                else:
                    all_settings.append(self._build_struct(index, input_val))

            msg.raw_array = [val for val in all_settings
                             if isinstance(val, dpm_protocol.RawSetting_struct)]
            msg.text_array = [val for val in all_settings
                              if isinstance(val, dpm_protocol.TextSetting_struct)]
            msg.scaled_array = [val for val in all_settings
                                if isinstance(val, dpm_protocol.ScaledSetting_struct)]

            (_, reply) = _rpc(self.con, self.dpm_task, msg)

            assert isinstance(reply, dpm_protocol.Status_reply)

            sts = acnet.Status(reply.status)
            if sts.is_fatal():
                raise sts

    # Starts data acquisition.

    def start(self, model=None):
        """Activate the list.

        Informs DPM to begin acquisition using the current list of
        requests. Use the 'pending' method to retrieve the replies as
        they stream in.
        """

        # Build up the remote list. First clear it and then add the
        # current list.

        self._clear_remote()
        for (tag, drf) in self.dev_list:
            self._add_remote(tag, drf)

        # Now start the data aquisition.

        msg = dpm_protocol.StartList_request()
        msg.list_id = self.list_id
        if model is not None:
            msg.model = model

        (_, reply) = _rpc(self.con, self.dpm_task, msg)

        assert isinstance(reply, dpm_protocol.StartList_reply)

        # All is good. Update our tag modifiers so we ignore any stale
        # data in the pipeline.

        self.curr_count = self.next_count
        self.next_count = (self.next_count + 1) % 4

    # Converts incoming replies from DPM into objects we can process
    # (otherwise there's a large set of objects to handle -- we map
    # all of them into a simpler, Python-esque object.)

    def stop(self):
        msg = dpm_protocol.StopList_request()
        msg.list_id = self.list_id

        _rpc(self.con, self.dpm_task, msg)

    def _xlat_reply(self, msg):
        if isinstance(msg, dpm_protocol.Status_reply):
            if msg.ref_id % 4 == self.curr_count:
                return ItemStatus(int(msg.ref_id / 4), acnet.Status(msg.status))
        elif isinstance(msg, dpm_protocol.ApplySettings_reply):
            all_replies = []
            for reply in msg.status:
                if reply.ref_id % 4 == self.curr_count:
                    all_replies.append(ItemStatus(
                        int(reply.ref_id / 4), acnet.Status(reply.status)))
            if len(all_replies) != 0:
                return all_replies
        elif isinstance(msg, dpm_protocol.TimedScalarArray_reply):
            if msg.ref_id % 4 == self.curr_count:
                return ItemData(int(msg.ref_id / 4), msg.timestamp, msg.cycle, msg.data, msg.micros)
        elif (isinstance(msg, (
                dpm_protocol.AnalogAlarm_reply,
                dpm_protocol.BasicStatus_reply,
                dpm_protocol.DigitalAlarm_reply,
                dpm_protocol.Raw_reply,
                dpm_protocol.ScalarArray_reply,
                dpm_protocol.Scalar_reply,
                dpm_protocol.TextArray_reply,
                dpm_protocol.Text_reply
        ))):
            if msg.ref_id % 4 == self.curr_count:
                return ItemData(int(msg.ref_id / 4), msg.timestamp, msg.cycle, msg.data)
        return None


class Polling(Connection):
    """Creates a polling interface to retrieve DPM data.
    """

    def __init__(self, con=None, task_node=None):
        Connection.__init__(self, con, task_node)

    def _alt_open_list(self):
        self._open_list(False)

    # Returns a generator which iterates across pending DPM replies.

    def pending(self):
        """Returns a generator which provides currently pending replies from
        DPM.

        Scripts can call this method in a 'for-in' loop to process any
        pending replies which have arrived since the last call.

        This method is not thread-safe.
        """
        for reply in self.replies:
            if reply is not None:
                (_, pkt) = reply
                try:
                    msg = dpm_protocol.unmarshal_reply(iter(pkt))
                except dpm_protocol.ProtocolError as error:
                    logging.warning('error marshalling reply: %s', error)
                    raise acnet.Status.ACNET_RPLYPACK
                else:
                    reply = self._xlat_reply(msg)
                    if reply is not None:
                        yield reply
            else:
                return


class Blocking(Connection):
    """Creates a blocking interface to retrieve DPM data.

    Use this when the main loop of the script is driven when data
    arrives.

    This method is not thread-safe.
    """

    def __init__(self, con=None, task_node=None):
        Connection.__init__(self, con, task_node)

    def _alt_open_list(self):
        self._open_list(True)

    def process(self, model=None):
        """Process all replies from DPM, blocking if none are available.
        """
        self.start(model)
        for reply in self.replies:
            (_, pkt) = reply
            try:
                msg = dpm_protocol.unmarshal_reply(iter(pkt))
            except dpm_protocol.ProtocolError as error:
                logging.warning('error unmarshalling reply: %s', error)
                raise acnet.Status.ACNET_RPLYPACK

            reply = self._xlat_reply(msg)
            if reply is not None:
                yield reply

        self.stop()
