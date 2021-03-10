import unittest
import json

from edf import EdfRecording, Scaler

# TODO: We need config for signal that are float - we need resolution, which is not documented in the EDF header
# TODO: It should be assumed that all signals are numerical, and special instructions can be given for parsing the signal
# samples, such as using call back function for special date time kind of values

# The result of an EdfRecording parsing is a large and complex object. If the entire output of parsed result is
# captured for verification, we'll end up having un-forgiving large files. For example output of PSG.edf could be
# near 500MB, 10 times the original in EDF format, 50MB. Thus, for verification purpose, we will capture portions
# of the whole Edf object, such as just the header, or just the metadata of all signals, or just the values of a specific signal.
HEADER_ONLY_EXPORT_OPTION = {"header": "true"}
ALL_METADATA_EXPORT_OPTION = {'slicing_by_index': -1, 'signal': ['metadata']}
FIRST_SIGNAL_VALUES_OPTION = {'slicing_by_index': [0], 'signal': ['values']}

SMALL_FILE_EXPORTED_OPTION = {'header': 'true', 'slicing_by_index': -1, 'signal': ['metadata', 'values']}
LARGE_FILE_EXPORTED_OPTION = {'header': 'true', 'slicing_by_index': [0], 'signal': ['metadata', 'values']}

class TestEdfParser(unittest.TestCase):

    def test_parse_file_at_once(self):
        for test_input in ['events.edf', 'periodic_6minutes.edf', 'Hypnogram.edf']:
        # for test_input in ['periodic_6minutes.edf']:
            input_file = f'tests/data/input/{test_input}'
            expected_output_file = input_file.replace('/input/', '/output/').replace('.edf', f'.json')

            sut = EdfRecording().parse_file_at_once(input_file)

            actual = sut.to_json_object(SMALL_FILE_EXPORTED_OPTION)
            # print(json.dumps(actual, indent=2))

            with open(expected_output_file, 'r') as expected_file:
                expected = json.loads(expected_file.read())

            self.assertEqual(expected, actual)

    # PSG.edf is a large recording (50M in edf format, representing 23 hours of recording at rather high sampling rate)
    # This is why this example is used to demonstrate the streaming option of EDF parsing
    def test_stream(self):
        extract_duration = EdfRecording.DEFAULT_DURATION_IN_SECONDS / 3 # 20 minutes only to limit out file size
        with EdfRecording().open('tests/data/input/PSG.edf') as sut:
            sut.stream() # header
            sut.stream() # metadata
            sut.stream(extract_duration) # first 20 minutes of recording

            actual = sut.to_json_object(LARGE_FILE_EXPORTED_OPTION)
            # print(json.dumps(actual, indent=2))

            with open('tests/data/output/PSG.json', 'r') as expected_file:
                expected = json.loads(expected_file.read())
                self.assertEqual(expected, actual)

            # 23 hours is a slightly over-estimate of the length of this recording "PSG.edf"
            approximated_recording_length = 23 * EdfRecording.DEFAULT_DURATION_IN_SECONDS
            max_expected_attempts = approximated_recording_length / extract_duration
            while not sut.is_done() and max_expected_attempts > 0:
                sut.stream(extract_duration)
                max_expected_attempts -= 1
            self.assertEqual(True, sut.is_done())


    # "Physical minimum": "-100.00 ", "Physical maximum": "100.00  ", "Digital minimum": "-32768  ", "Digital maximum": "32767   "
    def test_scaler(self):
        self.assertAlmostEqual(Scaler("100.00  ", "-100.00 ", "32767   ", "-32768  ").scale(550), 1.68)
        self.assertEqual(Scaler("100.00  ", "-100.00 ", "32767   ", "32767  ").scale(550), 550)
        self.assertEqual(Scaler("100.00  ", "100.00 ", "32767   ", "-32768  ").scale(550), 100.0)

if __name__ == '__main__':
    unittest.main(verbosity=2)
