curl localhost:8080 \
  -X POST \
  -H "Content-Type: application/json" \
  -H "ce-id: 123451234512345" \
  -H "ce-specversion: 1.0" \
  -H "ce-time: 2020-01-02T12:34:56.789Z" \
  -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
  -H "ce-source: //pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC" \
  -d '{
        "message": {
          "data": "ewogICAibWFudWZhY3R1cmVyX2lkIjoiMDE4OTEwOTgwIiwKICAgInNvdXJjZV90eXBlIjoidHJhY2tpbmctZGV2aWNlIiwKICAgInN1YmplY3RfbmFtZSI6IkxvZ2lzdGljcyBUcnVjayBBIiwKICAgInJlY29yZGVkX2F0IjoiMjAyMy0wMi0xNiAxMDozODowMCswMjowMCIsCiAgICJsb2NhdGlvbiI6ewogICAgICAibG9uIjozNS40MzkxLAogICAgICAibGF0IjotMS41OTA4MwogICB9LAogICAiYWRkaXRpb25hbCI6ewogICAgICAidm9sdGFnZSI6IjcuNCIsCiAgICAgICJmdWVsX2xldmVsIjo3MSwKICAgICAgInNwZWVkIjoiNDEga3BoIgogICB9Cn0=",
          "attributes": {
             "integration_id":"36485b4f-88cd-49c4-a723-0ddff1f580c4",
             "outbound_config_id":"1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
             "observation_type":"ps"
          }
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
      }'
