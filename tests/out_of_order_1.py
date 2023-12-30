import unittest

import apache_beam as beam
from apache_beam.testing import test_stream
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from utils import date_time_utils as ds_utils


class OutOfOrderTest(unittest.TestCase):
    def test_out_of_order_data(self):
        stream = test_stream.TestStream()

        print("")
        # First element arrives 15s after its "data" time.
        stream.advance_watermark_to(ds_utils.ts("12:00:45"))
        elem_1_time = "12:00:30"
        elem_1 = {"timestamp": ds_utils.ts(elem_1_time), "time": elem_1_time, "value": 1}
        stream.add_elements([elem_1])

        # Next element 5 seconds after its data time and advances output watermark past the previous window.
        stream.advance_watermark_to(ds_utils.ts("13:01:05"))
        elem_2_time = "13:01:01"
        elem_2 = {"timestamp": ds_utils.ts(elem_2_time), "time": elem_2_time, "value": 2}
        stream.add_elements([elem_2])

        # Late element from the first window arrives.
        stream.advance_watermark_to(ds_utils.ts("13:01:56"))
        elem_3_time = "12:00:31"
        elem_3 = {"timestamp": ds_utils.ts(elem_3_time), "time": elem_3_time, "value": 3}
        stream.add_elements([elem_3])
        # stream.advance_processing_time(ts("14:00:31"))
        stream.advance_watermark_to_infinity()

        with TestPipeline() as p:
            output = (
                p
                | "stream" >> stream
                | "rewrite timestamps"
                >> beam.Map(lambda e: beam.window.TimestampedValue(e, e["timestamp"]))
                | beam.ParDo(ToTupleTest())
                | "window" >> beam.WindowInto(beam.window.FixedWindows(60))
                | "add dummy key"
                >> beam.Map(lambda elem: ("sample-key-the-same-here", elem))
                | "group" >> beam.GroupByKey()
                # | "remove dummy key" >> beam.Map(lambda elem: elem[1])
                # | "print 1" >> beam.Map(print)
                # WHEN DUMMY KEY REMOVED !!!
                # | "sum" >> beam.Map(lambda elems: sum([e["value"] for e in elems]))
                # WHEN DUMMY KEY NOT REMOVED !!!
                # that's because elems is ie: ('sample-key-the-same-here', [{'time': 946728030.0, 'value': 1}, {'time': 946728031.0, 'value': 3}])
                # so it's tuple, and we are interested in 2nd element of it
                | "sum" >> beam.Map(lambda elems: sum([e["value"] for e in elems[1]]))
                # | "print" >> beam.Map(print)
            )

            #count_test = output | beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            #count_test = output | beam.combiners.Count.Globally()
            #print(f'count_test: {count_test}')
            # only 1 active assert_that possible ?
            # with both asserts active there is the error:
            # A transform with label "assert_that" already exists in the pipeline
            assert_that(output, equal_to([4, 2]))
            #assert_that(count_test, equal_to([3]))
            


    def test_window_simple_with_global_false(self):
        print("-------------")
        stream = test_stream.TestStream()
        stream.advance_watermark_to(ds_utils.ts("12:00:30"))
        stream.add_elements([1])
        stream.advance_watermark_to_infinity()
    
        with TestPipeline() as p:
            output = (
                p
                | "stream" >> stream
                | "window" >> beam.WindowInto(beam.window.FixedWindows(60))
                | "add dummy key" >> beam.Map(lambda elem: (None, elem))
                | "group" >> beam.GroupByKey()
                | "remove dummy key" >> beam.Map(lambda elem: elem[1])
            )
            # this syntax: ([1], [946724400.0, 946724460.0)) represents a half-open time interval [start, end).
            # Failed assert: [946724400.0, 946724460.0] == [([1], [946724400.0, 946724460.0))],
            # assert_that(
            #     output | beam.Map(lambda x, w=beam.DoFn.WindowParam: (x, w)),
            #     equal_to([946724400.0, 946724460.0])
            # )

    # class RecordFn(beam.DoFn):
    #     def process(
    #         self, element=beam.DoFn.ElementParam, timestamp=beam.DoFn.TimestampParam
    #     ):
    #
    #         yield ("key", (element, element["time"]))


class ToTupleTest(beam.DoFn):
    def process(self, element):
        # print("processing..................................")
        # print(f"element: {element} - {datetime.datetime.fromtimestamp(element['time'])}")
        yield element
