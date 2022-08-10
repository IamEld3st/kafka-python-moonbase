from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer('rover-zonechange',
                         bootstrap_servers='rover-cluster-kafka-bootstrap:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

route = [
    moon_outpost_a,
    moon_checkpoint_a,
    moon_outpost_b,
    moon_checkpoint_b,
    ...
]

last_seen = {}

current_travel_time = {
    "moon_outpost_a->moon_checkpoint_a": {
        duration: 50.2,
    },
    {...}
}

predicted_times = {
    "rover_id":{
        moon_outpost_a: timestamp,
        moon_checkpoint_a: timestamp,
        ...
    }
}

for msg in consumer:
    data = msg.value

    if data['previousZoneId']:
        print(f"Rover {data['carId']} left {data['previousZoneId']} at {time.time()}")
        last_seen[data['carId']] = {
            'last_zone': data['previousZoneId'],
            'timestamp': time.time()
        }
        
    elif data['nextZoneId']:
        print(f"Rover {data['carId']} entered {data['nextZoneId']} at {time.time()}")
        if data['carId'] in last_seen:
            print(f"  - This rover was last seen in {last_seen[data['carId']]['last_zone']} trip took {time.time()-last_seen[data['carId']]['timestamp']}")
            # Calculate travel time between points
            # Update predicted times using new durations
            #   - using last_seen_at timestamp + the travel time should create info about when is the aproximate arrival time
        
    else:
        print('Zone update malformed.')