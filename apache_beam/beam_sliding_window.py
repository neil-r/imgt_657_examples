import apache_beam as beam
from apache_beam import Windowing
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterProcessingTime, AfterCount, AccumulationMode

def sliding_window_example():
    # Sample data for the pipeline
    data = [
        ('2023-07-31', 10),
        ('2023-07-31', 15),
        ('2023-07-31', 20),
        ('2023-07-31', 25),
        ('2023-07-31', 30),
        ('2023-07-31', 35),
        ('2023-07-31', 40),
        ('2023-07-31', 45),
        ('2023-07-31', 50),
    ]

    # Create a PipelineOptions object to specify the pipeline configuration
    pipeline_options = PipelineOptions()

    # Create a pipeline using the DirectRunner (local execution)
    with beam.Pipeline(runner='DirectRunner', options=pipeline_options) as p:
        # Read the data from the list
        input_data = p | beam.Create(data)

        # Apply sliding windows of size 5 seconds and sliding interval of 2 seconds
        windowed_data = input_data | beam.WindowInto(beam.window.SlidingWindows(size=5, period=2))
        # windowed_data = input_data | beam.WindowInto(beam.window.FixedWindows(size=5))

        # Calculate the sum of values within each sliding window
        def calculate_sum(element):
            window, values = element
            return {'window': window, 'sum': sum(values)}

        summed_data = windowed_data | beam.GroupByKey() | beam.Map(calculate_sum)

        # Output the results
        summed_data | beam.Map(print)

if __name__ == '__main__':
    sliding_window_example()