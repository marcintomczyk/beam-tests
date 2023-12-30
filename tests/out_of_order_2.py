import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import (PipelineOptions,
                                                  StandardOptions)
from apache_beam.testing import test_stream
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.trigger import (AccumulationMode, AfterCount,
                                            Repeatedly, AfterWatermark, AfterProcessingTime)
from apache_beam.transforms.window import FixedWindows, TimestampedValue, Sessions

# remember: late data is every data element that has a timestamp that is behind the watermark

class OutOfOrderTest2(unittest.TestCase):
    def test_out_of_order_data_2(self):
        stream = (
            test_stream.TestStream()
            .add_elements(
                [TimestampedValue("in_time_1", 0), TimestampedValue("in_time_2", 0)]
            )
            .advance_watermark_to(9)
            .advance_processing_time(9)
            .add_elements([TimestampedValue("late_but_in_window", 8)]) # window still not closed - because we advanced to 9 and window 'ends' when 10
            .advance_watermark_to(10)
            .advance_processing_time(10)
            .add_elements([TimestampedValue("in_time_window2", 12)]) # it's in another window and we have a new one starting from 10 and ending when 20
            .advance_watermark_to(20)  # Past window time
            .advance_processing_time(20)
            # when we change 20 -> 21 above - the message 'in_time_window2_2' will be discarded - because lateness was set to 0
            # if we change allowed lateness from 0 to 1 when we have 21 above the message 'in_time_window2_2' will be present
            # if we change allowed lateness from 0 to 11 the message 'late_window_closed' will be visible too - it was initially in the window 0-10 + 11 allowed lateness so it's 21
            # and we still have 21 as the watermark timestamp
            # but if we have allowed lateness set to 10 - the message 'late_window_closed' will be discarded
            .add_elements(
                [
                    TimestampedValue("late_window_closed", 9),
                    TimestampedValue("in_time_window2_2", 12), 
                ]
            )
            .advance_watermark_to_infinity()
        )

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        print("------------------------")

        with TestPipeline(options=options) as p:
            records = (
                p
                | stream
                | beam.ParDo(RecordFn())
                | beam.WindowInto(
                    FixedWindows(10),
                    #Sessions(10),
                    allowed_lateness=0,
                    #trigger=Repeatedly(AfterCount(1)),
                    trigger=AfterWatermark(),
                    #trigger=Repeatedly(AfterWatermark(late=AfterCount(1))),
                    #trigger=Repeatedly(AfterProcessingTime(10)), # does not finish the pipeline
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                # remove group by key and you see all messages
                | beam.ParDo(SplitDroppableFn())
                | beam.GroupByKey()
                #| beam.Map(print)
                #| beam.ParDo(print_output)
            )


# note: it's called after groupBy!!! so checking if the timestamp is withing window's boundaries gives us nothing
# late elements will be already dropped in the groupByKey step
def print_output(
    element,
    window=beam.DoFn.WindowParam,
    pane_info=beam.DoFn.PaneInfoParam,
    timestamp=beam.DoFn.TimestampParam,
):
    print(window.start.to_utc_datetime())
    print(window.end.to_utc_datetime())
    print(pane_info)
    print(timestamp)
    print(element)
    print("-----------------")


class RecordFn(beam.DoFn):
    def process(
        self, element=beam.DoFn.ElementParam, timestamp=beam.DoFn.TimestampParam
    ):
        yield ("key", (element, timestamp))

class SplitDroppableFn(beam.DoFn):
    def process(
        self, 
        element=beam.DoFn.ElementParam,
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
        timestamp=beam.DoFn.TimestampParam
    ):
        print(f"checking if the message {element} timestamp is too old for our purposes")
        print(f"timestamp - window.end:  {timestamp} - {window.end}")
        #print(f'PANE: {pane_info}')
        # DOES NOT WORK - 9 is still checked against Timestamp(10) - not 20
        # TODO: maybe we should check the timestamp versus the current time ?
        # this could be configurable so we would know that we have windows with 10 minutes length + some allowed lateness
        # so if we the message with the timestamp earlier than this currently calculated time we know it's 'droppable' element
        # TODO: would be possible to compare the pcoll before groupby and after ? - we need to store them somewhere for a time being
        # there should be 'per key' - because groupBy would emit grouped ones - this might not be a good idea...
        print(f'window: {window}')
        if timestamp <= window.end:
            print("WITHIN WINDOW")
        else:
            print("NOT IN WINDOW")


        #yield ("key", (element, timestamp))
        yield element


    # If we have trigger trigger.Repeatedly(trigger.AfterCount(1)), all elements are fired as they come,
    # with no dropped element (but late_window_closed which is expected as it was late):
    #
    # ('key', [('in_time_1', Timestamp(0)), ('in_time_2', Timestamp(0))])  # this two are together since they arrived together
    # ('key', [('late_but_in_window', Timestamp(8))])
    # ('key', [('in_time_window2', Timestamp(12))])
    # ('key', [('in_time_window2_2', Timestamp(12))])
    # If we use trigger.AfterCount(1) (no repeatedly), we only get the first elements that arrived in the pipeline:
    #
    # ('key', [('in_time_1', Timestamp(0)), ('in_time_2', Timestamp(0))])
    # ('key', [('in_time_window2', Timestamp(12))])
    # Note that both in_time_(1,2) appear in the first fired pane because the arrived at the same time (0), were one of them appear later it would have been dropped.
