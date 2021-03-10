#!/usr/bin/env python
# encoding: utf-8

from __future__ import division

import math
import struct

SIGNAL_HEADER_FIELDS = ('Label', 'Transducer Type', 'Physical dimension', 'Physical minimum',
                      'Physical maximum', 'Digital minimum', 'Digital maximum', 
                      'Prefiltering', 'Samples', 'Reserved')
SIGNAL_HEADER_FIELDS_SIZE = (16, 80, 8, 8, 8, 8, 8, 80, 8, 32)
UNDEFINED = -32768


# Not so safe a method for transforming from byte array to string. Use it only if you know surely that the byte array contains string
# Otherwise needs more sophysticated encoding / decoding scheme
def byte_array_to_string(bytesData):
    return "".join(map(chr, bytesData))


def string_to_numerical(str):
    try:
        return int(str.strip())
    except ValueError:
        pass

    try:
        return float(str)
    except ValueError:
        print(f'[ERROR] {str} cannot be interpreted as numerical - int or float, used UNDEFINED {UNDEFINED}!')
        return UNDEFINED


# Returns hex string to represent number in hexadecimal
def numerical_to_hex_string(val, nbits):
    return hex((val + (1 << nbits)) % (1 << nbits))


# pmax, pmin, dmax, dmin - physical max, min and digital max, min
# scaled_value = delta_P * (d_value - d_min) / delta_D + p_min
class Scaler(object):
    def __init__(self, pmax, pmin, dmax, dmin):
        try:
            p_max, self.p_min, d_max, self.d_min = map(float, (pmax, pmin, dmax, dmin))
            self.p_delta = p_max - self.p_min
            self.d_delta = d_max - self.d_min

        except TypeError:
            self.p_delta, self.d_delta, self.p_min, self.d_min = [None, None, None, None]

    def scale(self, dvalue):
        return float('%.3f' % (self.p_delta * (dvalue - self.d_min) / self.d_delta + self.p_min)) \
            if (self.d_delta > 0 or self.d_delta < 0) else dvalue


class EdfHeader(object):

    STRING_FIELDS = ['version', 'patient', 'recordId', 'startDate', 'startTime', 'reserved']
    NUM_FIELDS = ['headerSize', 'number_records', 'recordDuration', 'number_signals']
    
    def __init__(self):
        self.fields = ('version', 'patient', 'recordId', 'startDate', 'startTime', 'headerSize', 
                       'reserved', 'number_records', 'recordDuration', 'number_signals')
        self.field_descriptions = ('Version Format', 'Local Patient Info', 'Local Recording Info',
                                   'Recording Start Date', 'Recording Start Time',
                                   'Recording Header Bytes', 'Reserved', 'Num Data Records',
                                   'Recording Duration (s)', 'Num Signals')
        self.fields_sizes = (8, 80, 80, 8, 8, 8, 44, 8, 8, 4)
        self.size = sum(self.fields_sizes)
        self.header_dict = {}

    def __getitem__(self, field):
        return self.header_dict[field]

    def to_json_object(self):
        header_as_json = {'total_header_size': self.size}
        for item in zip(self.fields, self.field_descriptions, self.fields_sizes):
            field, description, size = item
            header_as_json[field] = {
                'field_description': description,
                'field_size': size,
                'value': self.header_dict.get(field)
            }
        return header_as_json

    def get(self, field):
        return self.header_dict[field] if field in self.header_dict.keys() else None

    def parse(self, header):
        if len(header) != self.size:
            raise Exception(f'Wrong size of header received, expect header size: {self.size}, actual: {len(header)}')
        descriptor = " ".join([("%ss") % (s) for s in self.fields_sizes])
        s = struct.Struct(descriptor)
        field_values = s.unpack(header)
        self.header_dict = dict(zip(self.fields, field_values))

        # Iterate all fields, convert to numerical if they belong to numerical properties
        for k, v in self.header_dict.items():
            self.header_dict[k] = string_to_numerical(v) if k in EdfHeader.NUM_FIELDS else  byte_array_to_string(v)


# There is no easy way to know if a signal is numerical. We need to pass in the config for parsing so the parser knows
# if it treats the signal as numerical. In the absence of config, all signals should be treated as numerical
class Signal(object):
    def __init__(self):
        self.meta_data_fields = SIGNAL_HEADER_FIELDS
        self.metadata = {}
        # All samples of a signal for the entire recording will be organised in a list of records
        # Each record is a tuple of real data samples. This make the representation closely match to the EDF binary representation.
        # Example - For Astral detailed data - a record represents one minute of recording. At sampling rate of 40Hz, this means
        # (60*40) samples in a record. So, for a recording of 10 minutes, this "self.samples" will be a list of 60 tuples, with each tuple
        # holding (60*40) samples [(...), (...), (...)]
        self.samples = []
        self.scaler = None

    # This implementation hasn't yet supported slicing per hours of recording, or do we want raw_samples?
    def to_json_object(self, options = ('metadata', 'values')):
        signal_as_json = {}
        if 'metadata' in options:
            signal_as_json.update({
                'metadata': self.metadata
            })
        if 'values' in options:
            signal_as_json.update({
                'values': self.format_samples()
            })

        return signal_as_json

    def is_numerical(self):
        # EVENT_EDF has only annotation and empty body for Transducer Type and Physical dimension
        return self.metadata['Transducer Type'].strip() or self.metadata['Physical dimension'].strip()

    def get_record(self, index):
        return self.samples[index]
    
    def get_samples(self):
        samples = []
        for rec in self.samples:
            for sample in rec:
                samples.append(sample)
        return samples
    
    def total_number_of_samples(self):
        return self.number_samples_per_record() * self.number_records()
    
    def get_name(self):
        return self.metadata.get('Label', 'UNKNOWN')

    def number_samples_per_record(self):
        return int(self.metadata.get('Samples', 0))
    
    def number_records(self):
        return len(self.samples)

    def set_metadata(self, field, value):
        self.metadata[field] = value
        if len(self.metadata.keys()) == len(self.meta_data_fields):
            self.scaler = Scaler(self.metadata['Physical maximum'], self.metadata['Physical minimum'],
                                 self.metadata['Digital maximum'], self.metadata['Digital minimum'])

    # Scaling happens only when we serialize the edf object
    def format_samples(self, scale=True, char_for_undefined=None):
        if not scale and not char_for_undefined:
            return self.samples

        formatted_samples = []
        for s in self.samples:
            outcome = s
            if char_for_undefined:
                outcome = map(lambda x: char_for_undefined if x == UNDEFINED else x, s)
            if not self.is_numerical():
                outcome = map(lambda x: numerical_to_hex_string(x, 16), outcome)
            elif scale:
                outcome = map(self.scaler.scale, outcome)

            formatted_samples.append(list(outcome))

        return formatted_samples

    def add_samples_for_one_record(self, sample_data):
        self.samples.append(sample_data)


class EdfRecording(object):

    STAGE_OPEN = 'open'
    STAGE_HEADER = 'header'
    STAGE_METADATA = 'metadata'
    STAGE_PARSING = 'parsing'
    STAGE_DONE = 'exhausted'

    PARSER_STREAM_STAGES = (STAGE_OPEN, STAGE_HEADER, STAGE_METADATA, STAGE_PARSING, STAGE_DONE)

    # Exporting all signals result in a huge text file. Use "selected_signals" to specify the indices of wanted signals
    # Choose "header" for sneak peek into the header part
    # Choose slicing_by_index = -1 if we want all indices (i.e. all signals)
    # Choose slicing_by_hours = -1 if we want the complete recording
    # Under 'signal', choose just 'metadata' to see the metadata of signals, or just 'values' or both
    DEFAULT_EXPORT_OPTIONS = {
        'header': 'true',
        'slicing_by_index': [0],
        'slicing_by_hours': 0,
        'signal': ['metadata', 'values']
    }
    DEFAULT_DURATION_IN_SECONDS = 1 * 3600

    def __init__(self):
        self.binary_content = None
        self.edf_file = None
        self.status = None
        self.header = None
        self.number_signals = None
        self.number_records = None
        self.signal_header_size = None
        self.samples_per_record = None
        self.record_size = None
        self.signals = []
        
    def to_json_object(self, options = DEFAULT_EXPORT_OPTIONS):
        edf_as_json = {}

        if 'header' in options.keys():
            edf_as_json.update({
                'header': self.header.to_json_object(),
                'number_signals': self.number_signals,
                'number_records': self.number_records,
                'signal_header_size': self.signal_header_size,
                'samples_per_record': self.samples_per_record,
                'record_size': self.record_size
            })

        if 'slicing_by_index' in options.keys() and 'signal' in options.keys():
            signals = []
            wanted_indices = options['slicing_by_index']
            for i in range(0, len(self.signals)):
                if wanted_indices == -1 or i in wanted_indices:
                    signals.append(self.signals[i].to_json_object(options['signal']))

            edf_as_json.update({
                'signals': signals
            })

        return edf_as_json

    def is_done(self):
        return self.status == EdfRecording.STAGE_DONE

    def parse_binary(self, blob):
        self.binary_content = blob
        self.status = EdfRecording.STAGE_OPEN
        self.__debug_log__()
        while self.status != EdfRecording.STAGE_DONE:
            self.stream(duration = EdfRecording.DEFAULT_DURATION_IN_SECONDS)
        return self.status

    def parse_file_at_once(self, file_name):
        with open(file_name, 'rb') as edf_file:
            self.parse_binary(edf_file.read())
        return self

    def open(self, file_name):
        self.edf_file = open(file_name, 'rb')
        self.status = EdfRecording.STAGE_OPEN
        self.__debug_log__()
        return self

    def stream(self, duration = None):
        if self.status == EdfRecording.STAGE_OPEN:
            self.header = EdfHeader()

            bytes_to_read = self.header.size
            self.status = EdfRecording.STAGE_HEADER

            self.header.parse(self.__extract__(bytes_to_read))
            self.number_signals = int(self.header.get('number_signals'))
            self.number_records = int(self.header.get('number_records'))
            self.signal_header_size = self.number_signals * sum(SIGNAL_HEADER_FIELDS_SIZE)
            self.samples_per_record = 0
            self.record_size = 0
            self.signals = []
            for _ in range(0, self.number_signals):
                self.signals.append(Signal())

        elif self.status == EdfRecording.STAGE_HEADER:
            bytes_to_read = self.signal_header_size
            self.status = EdfRecording.STAGE_METADATA
            self.__parse_metadata__(self.__extract__(bytes_to_read))

        elif self.status in (EdfRecording.STAGE_METADATA, EdfRecording.STAGE_PARSING):
            self.status = EdfRecording.STAGE_PARSING
            bytes_to_read = self.record_size

            if math.isclose(self.header.get('recordDuration'), 0):
                # The "hypnogram.edf" has record duration = 0! Yet to find out why. (EDF+C ? where samples are not spaced equally)
                records_to_read = 1
            else:
                records_to_read = int(self.record_size if not duration else duration / self.header.get('recordDuration'))
            for i in range(0, records_to_read):
                bytes_extracted = self.__extract__(bytes_to_read)
                if len(bytes_extracted) > 0:
                    self.__parse_one_record__(bytes_extracted)
                else:
                    self.status = EdfRecording.STAGE_DONE

        elif self.status == EdfRecording.STAGE_DONE:
            print('[WARN] Stream is already exhausted! Attempts to go pass stream end!')

        self.__debug_log__()
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.edf_file:
            self.edf_file.close()

    def __debug_log__(self):
        # print(f'{len(self.binary_content)} bytes, status = {self.status}') if self.binary_content else print(f'{self.status}')
        pass

    def __extract__(self, count):
        if self.binary_content:
            returned_bytes = self.binary_content[:count]
            self.binary_content = self.binary_content[count:]
            return returned_bytes
        elif self.edf_file:
            return self.edf_file.read(count)
        else:
            return b''

    def __parse_metadata__(self, header_bytes):
        end = 0
        for i, l in enumerate(SIGNAL_HEADER_FIELDS):
            start = end
            field_size = SIGNAL_HEADER_FIELDS_SIZE[i]
            field_descriptor = "%ss " %(field_size) * self.number_signals
            end = start + field_size * self.number_signals

            s = struct.Struct(field_descriptor)
            metadata_as_bytes = s.unpack(header_bytes[start:end])
            metadata=tuple(map(byte_array_to_string, metadata_as_bytes))

            for s, m in zip(self.signals, metadata):
                s.set_metadata(l, m)

        self.samples_per_record = sum([int(s.number_samples_per_record()) for s in self.signals])

        # Each sample takes 2 bytes, which applies for ALL signals
        # self.samples_per_record represents the total number of samples of ALL signals in a record. This is used to firgured out the record
        # Within a record, different signals can be having different sampling rate, so signal 1 may have 100 samples in a record & signal 2
        # has 10 while signal 3 has just 1. It's up to the designer of the file, but signals are not guaranteed to be of same sampling rate.
        self.record_size = 2 * self.samples_per_record

    def __parse_one_record__(self, sample):
        if sample:
            record_descriptor = "h " * self.samples_per_record
            s = struct.Struct(record_descriptor)
            data = s.unpack(sample)

            start = 0
            for s in self.signals:
                end = start + s.number_samples_per_record()
                s.add_samples_for_one_record(data[start:end])
                start = end

    # This method is no longer used after we changed for streaming of EDF parsing
    # However it appears much more performing comparing to streaming method which will eventuate to parsing records one by one
    def __parse_whole_signal_value_pool__(self, samples):
        # Figuring out the start and end index of each record block
        indexes = map(lambda x:(x, x + self.record_size), range(0, len(samples), self.record_size))
        # Chopping out the block for one record from the whole "samples" pool
        oneSignalSampleBlock =  map(lambda s: samples[s], [slice(*s) for s in indexes])
        for s in oneSignalSampleBlock:
            self.__parse_one_record__(s)
