import threading
from generate_data import Generator_Sales_Data
from stream_process import PyFlinkConnector

if __name__ == "__main__":
    gen_data = Generator_Sales_Data(topic_name="test", bootstrap_servers="localhost:9092")
    process_data = PyFlinkConnector(topic_name="test", bootstrap_server="localhost:9092", db_mysql="Streaming-Project", index_es="streaming-project")
    print("==============================DDL Database=================================")
    process_data.ddl_mysql()
    process_data.ddl_es()
    print("===========================================================================")
    threads_tasks = [
        gen_data.main,
        process_data.create_flink_stream,
        process_data.stream_transactions_mysql,
        process_data.stream_transactions_elasticsearch
    ]

    for idx, task in enumerate(threads_tasks, start=1):
        thread = threading.Thread(target=task, name=f"Thread {idx}")
        thread.start()