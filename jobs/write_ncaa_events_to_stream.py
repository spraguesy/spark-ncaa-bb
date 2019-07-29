import kafka
import pandas as pd
import json
from time import sleep



NCAA_EVENT_PATH = "../docs/pbp000000000011.csv"
TOPIC_NCAA = "ncaa_events"


if __name__ == '__main__':

    # Read a CSV of events     
    events_df = pd.read_csv(NCAA_EVENT_PATH, header=0)

    # Shooting events    
    shooting_events = ['twopointmiss', 'twopointmade','threepointmiss','threepointmade']

    # Filter the events for only shooting events
    shooting_events_df = events_df[events_df.event_type.isin(shooting_events)]
    
    left_basket_shots_df = shooting_events_df[events_df.team_basket == "left"]

    # Take the first 500 shooting events
    shooting_event_list = left_basket_shots_df.to_json(orient='index')

    # Dictionary of Shooting events     
    shooting_json = json.loads(shooting_event_list)

    # Create the KafkaProducer
    producer = kafka.KafkaProducer(value_serializer=lambda event: event.encode("utf-8"))

    # Loop through teh 
    for key, v in shooting_json.items():
        event = {
            'player_id': v['player_id'],
            'player_full_name': v['player_full_name'],
            'shot_made': v['shot_made'],
            'three_point_shot': v['three_point_shot'],
        }
        producer.send(TOPIC_NCAA, value=json.dumps(event))
        sleep(0.1)
        