curl localhost:8080 \
  -X POST \
  -H "Content-Type: application/json" \
  -H "ce-id: 123451234512345" \
  -H "ce-specversion: 1.0" \
  -H "ce-time: 2023-03-09T12:34:56.789Z" \
  -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
  -H "ce-source: //pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC" \
  -d '{
        "message": {
          "data":"eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
          "attributes":{
             "observation_type":"ps",
             "device_id":"018910980",
             "outbound_config_id":"1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
             "integration_id":"36485b4f-88cd-49c4-a723-0ddff1f580c4",
             "tracing_context":"{}"
          }
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
      }'
# CemeraTrap
#  -d '{
#      "message": {
#        "data":"eyJmaWxlIjogImNhbWVyYXRyYXAuanBnIiwgImNhbWVyYV9uYW1lIjogIk1hcmlhbm8ncyBDYW1lcmEiLCAiY2FtZXJhX2Rlc2NyaXB0aW9uIjogInRlc3QgY2FtZXJhIiwgInRpbWUiOiAiMjAyMy0wMy0wNyAxMTo1MTowMC0wMzowMCIsICJsb2NhdGlvbiI6ICJ7XCJsb25naXR1ZGVcIjogLTEyMi41LCBcImxhdGl0dWRlXCI6IDQ4LjY1fSJ9",
#        "attributes":{
#          "observation_type":"ct",
#          "device_id":"Mariano Camera",
#          "outbound_config_id":"5f658487-67f7-43f1-8896-d78778e49c30",
#          "integration_id":"a244fddd-3f64-4298-81ed-b6fccc60cef8",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# GeoEvent
#  -d '{
#      "message": {
#        "data":"eyJ0aXRsZSI6ICJSYWluZmFsbCIsICJldmVudF90eXBlIjogInJhaW5mYWxsX3JlcCIsICJldmVudF9kZXRhaWxzIjogeyJhbW91bnRfbW0iOiA2LCAiaGVpZ2h0X20iOiAzfSwgInRpbWUiOiAiMjAyMy0wMy0wNyAxMToyNDowMi0wNzowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogLTU1Ljc4NDk4LCAibGF0aXR1ZGUiOiAyMC44MDY3ODV9fQ==",
#        "attributes":{
#          "observation_type":"ge",
#          "device_id":"003",
#          "outbound_config_id":"9243a5e3-b16a-4dbd-ad32-197c58aeef59",
#          "integration_id":"8311c4a5-ddab-4743-b8ab-d3d57a7c8212",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
