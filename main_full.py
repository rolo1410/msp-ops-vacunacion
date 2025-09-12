from lake.ingest_lake import process_data

if __name__ == "__main__":
    since = "2023-01-01"
    until = "2023-12-31"
    process_data(since, until, chunk_size=1000)