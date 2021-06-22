from google.cloud import bigquery

def create_tfl_data_set():
    client = bigquery.Client(project="tfl-stats")
    dataset_id = "{}.tfl_stats".format(client.project)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west2" #London
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))    

def create_tube_line_table():
    client = bigquery.Client(project="tfl-stats")
    table_id = "{}.{}.tube_lines".format(client.project, "tfl_stats")
    schema = [
      bigquery.SchemaField("tube_line_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("tube_line_name", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
    	"Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    	)

def create_tube_stations_table():
    client = bigquery.Client(project="tfl-stats")
    table_id = "{}.{}.tube_stations".format(client.project, "tfl_stats")
    schema = [
      bigquery.SchemaField("tube_line_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("tube_station_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("tube_station_name", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("lat", "FLOAT", mode="REQUIRED"),
      bigquery.SchemaField("lon", "FLOAT", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

def create_tube_crowding_table():
    client = bigquery.Client(project="tfl-stats")
    table_id = "{}.{}.tube_crowding".format(client.project, "tfl_stats")
    schema = [
      bigquery.SchemaField("tube_station_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("measurement_ts", "TIMESTAMP", mode="REQUIRED"),
      bigquery.SchemaField("crowding_percentage_of_base_line", "FLOAT", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

if __name__ == "__main__":
    create_tfl_data_set()
    create_tube_line_table()
    create_tube_stations_table()
    create_tube_crowding_table()