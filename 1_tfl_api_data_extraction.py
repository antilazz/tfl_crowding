import requests
import json
import time
import os

import pipeline_config as cfg

if __name__ == "__main__":
  headers = {
      "Accept" : "application/json",
      "Content-Type" : "application/json",
      "Cache-Control" : "no-cache",
      #the primary and secondary keys can both be used as app keys
      "app_key" : cfg.tfl_api_app_key()
  }
  lines  = []
  station_naptans = []
  base_dir =  cfg.output_file_dir()
  ########################  
  #1. get all tube lines
  ########################
  print("fetching all tube lines")
  r1 = requests.get("https://api.tfl.gov.uk/Line/Mode/tube", headers = headers)
  lines_data = r1.json()
  with open(os.path.join(base_dir, "tube_lines","tube_lines_test.json"), 'w') as f1:
    for line in lines_data:
      #need to write ndjson https://github.com/ndjson/ndjson-spec
      f1.write(json.dumps(line))
      f1.write('\n')  
      lines.append(line["id"])
  print(lines)  
  time.sleep(30)


  ########################
  #2. get all stations across all tube lines
  ########################
  for line in lines:
    print(f"fetching stops on line: {line}")
    r2 = requests.get(f"https://api.tfl.gov.uk/Line/{line}/StopPoints", headers = headers)
    route_data = r2.json()
    with open(os.path.join(base_dir, "tube_stations", f"{line}.json"), "w") as f2:
      for stop in route_data:
        station_naptans.append(stop["naptanId"])
        stop["lineId"] = line
        #need to write ndjson https://github.com/ndjson/ndjson-spec
        f2.write(json.dumps(stop))
        f2.write("\n")
    time.sleep(30)

  ########################
  #3. get crowding data for all tube stations
  ########################
  print(f"fetching data for {len(station_naptans)} stations")
  for station in station_naptans:
    print(f"fetching data for station: {station}")
    r3 = requests.get(f"https://api.tfl.gov.uk/crowding/{station}", headers = headers)
    crowding_data = r3.json()
    with open(os.path.join(base_dir, "tube_crowding",f"{station}.json"), "w") as f3:
      for day in crowding_data["daysOfWeek"]:
        day["tube_station_id"] = crowding_data["naptan"]
        #need to write ndjson https://github.com/ndjson/ndjson-spec
        f3.write(json.dumps(day))
        f3.write("\n")
    time.sleep(5)

  
