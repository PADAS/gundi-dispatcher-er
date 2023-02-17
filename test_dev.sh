curl -m 70 -X POST https://er-positions-dispatcher-bbfgemg6oq-uc.a.run.app \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-H "ce-id: 1234567890" \
-H "ce-specversion: 1.0" \
-H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
-H "ce-time: 2020-08-08T00:11:44.895529672Z" \
-H "ce-source: //pubsub.googleapis.com/projects/cdip-78ca/topics/destination-33c2605c-e528-4216-b602-c14dbe64dce5" \
-d '{
"message": {
          "data": "ewogICAibWFudWZhY3R1cmVyX2lkIjoiMDE4OTEwOTgwIiwKICAgInNvdXJjZV90eXBlIjoidHJhY2tpbmctZGV2aWNlIiwKICAgInN1YmplY3RfbmFtZSI6IkxvZ2lzdGljcyBUcnVjayBBIiwKICAgInJlY29yZGVkX2F0IjoiMjAyMy0wMi0xNiAxMDozODowMCswMjowMCIsCiAgICJsb2NhdGlvbiI6ewogICAgICAibG9uIjozNS40MzkxLAogICAgICAibGF0IjotMS41OTA4MwogICB9LAogICAiYWRkaXRpb25hbCI6ewogICAgICAidm9sdGFnZSI6IjcuNCIsCiAgICAgICJmdWVsX2xldmVsIjo3MSwKICAgICAgInNwZWVkIjoiNDEga3BoIgogICB9Cn0=",
          "attributes": {
             "integration_id":"00bcef14-6a25-411a-9c35-df1c63dc3b90",
             "outbound_config_id":"33c2605c-e528-4216-b602-c14dbe64dce5",
             "observation_type":"ps"
          }
        }
}'
