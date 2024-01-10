import pandas as pd

def update_metrics_summary():
    csv_file_path = "/home/stuproj/cs4224b/param_cassandra_transactions/clients.csv"
    df = pd.read_csv(csv_file_path)
    minimum_throughput = df['transaction_throughput'].min()
    maximum_throughput = df['transaction_throughput'].max()
    average_throughput = df['transaction_throughput'].mean()

    result_df = pd.DataFrame({"minimum_throughput": [minimum_throughput], "maximum_throughput": [maximum_throughput], "average_throughput": [average_throughput]})
    output_csv_file_path = "/home/stuproj/cs4224b/param_cassandra_transactions/throughput.csv"
    result_df.to_csv(output_csv_file_path, mode="w", header=True, index=False)

