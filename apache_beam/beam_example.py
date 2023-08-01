from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

def dataflow_modeling_example():
    # Sample data for the pipeline
    data = [
        ('Alice', {'score': 85}),
        ('Bob', {'score': 72}),
        ('Charlie', {'score': 93}),
        ('Alice', {'score': 90}),
        ('Bob', {'score': 68}),
        ('Charlie', {'score': 880}),
    ]

    # Create a PipelineOptions object to specify the pipeline configuration
    pipeline_options = PipelineOptions(
        # streaming=True,  # Set streaming to True for unbounded data sources
    )

    # Create a pipeline using the DirectRunner (local execution)
    with beam.Pipeline(runner='DirectRunner', options=pipeline_options) as p:
        # Read the data from the list
        input_data = p | beam.Create(data)

        # Group the data by name
        grouped_data = input_data | beam.GroupByKey()

        # Calculate the average score for each person
        def calculate_average_score(element):
            name, scores_d = element
            scores = [e["score"] for e in scores_d]
            average_score = sum(scores) / len(scores)
            return name, average_score

        average_scores = grouped_data | beam.Map(calculate_average_score)

        # Output the results
        average_scores | beam.Map(print)

if __name__ == '__main__':
    dataflow_modeling_example()