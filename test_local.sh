curl localhost:8080 \
  -X POST \
  -H "Content-Type: application/json" \
  -H "ce-id: 123451234512345" \
  -H "ce-specversion: 1.0" \
  -H "ce-time: 2023-07-31T10:01:56.789Z" \
  -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
  -H "ce-source: //pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC" \
  -d '{
      "message": {
        "data":"eyJ0aXRsZSI6ICJXaWxkZG9nIERldGVjdGVkIiwgImV2ZW50X3R5cGUiOiBudWxsLCAiZXZlbnRfZGV0YWlscyI6IHsic2l0ZV9uYW1lIjogIkNhbWVyYTJNIiwgInNwZWNpZXMiOiAiV2lsZGRvZyIsICJ0YWdzIjogWyJhZHVsdCIsICJtYWxlIl0sICJhbmltYWxfY291bnQiOiAxfSwgInRpbWUiOiAiMjAyMy0wNy0zMSAxNDowODowMCswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogLTcyLjcwNDQzNSwgImxhdGl0dWRlIjogLTUxLjY4ODY0OX0sICIiOiBudWxsfQ==",
        "attributes":{
          "gundi_version":"v2",
          "provider_key":"ddd0946d-15b0-4308-b93d-e0470b6d33b6",
          "gundi_id":"35a9c36b-e566-4365-af87-c0caa9323b3d",
          "related_to": "None",
          "stream_type":"ev",
          "source_id":"afa0d606-c143-4705-955d-68133645db6d",
          "external_source_id":"Xyz123",
          "destination_id":"338225f3-91f9-4fe1-b013-353a229ce504",
          "data_provider_id":"ddd0946d-15b0-4308-b93d-e0470b6d33b6",
          "annotations":"{}",
          "tracing_context":"{}"
        }
      },
      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
    }'
# Gundi v2 Event
#  -d '{
#      "message": {
#        "data":"eyJ0aXRsZSI6ICJBbmltYWwgRGV0ZWN0ZWQiLCAiZXZlbnRfdHlwZSI6ICJsZW9wYXJkX3NpZ2h0aW5nIiwgImV2ZW50X2RldGFpbHMiOiB7InNpdGVfbmFtZSI6ICJDYW1lcmEyQSIsICJzcGVjaWVzIjogIkxlb3BhcmQiLCAidGFncyI6IFsiYWR1bHQiLCAibWFsZSJdLCAiYW5pbWFsX2NvdW50IjogMn0sICJ0aW1lIjogIjIwMjMtMDYtMjMgMDA6NTE6MDArMDA6MDAiLCAibG9jYXRpb24iOiB7ImxvbmdpdHVkZSI6IDIwLjgwNjc4NSwgImxhdGl0dWRlIjogLTU1Ljc4NDk5OH19",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"awt",
#          "gundi_id":"23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
#          "related_to":"None",
#          "stream_type":"ev",
#          "source_id":"afa0d606-c143-4705-955d-68133645db6d",
#          "external_source_id":"Xyz123",
#          "destination_id":"338225f3-91f9-4fe1-b013-353a229ce504",
#          "data_provider_id":"ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "annotations":"{}",
#          "tracing_context":"{\"x-cloud-trace-context\": \"95f36c1f22b1cb599efc28243a631f7d/15139689239813763386;o=1\"}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# Gundi v2 Attachment
#  -d '{
#      "message": {
#        "data":"eyJmaWxlX3BhdGgiOiAiYXR0YWNobWVudHMvZjFhODg5NGItZmYyZS00Mjg2LTkwYTAtOGYxNzMwM2U5MWRmXzIwMjMtMDYtMjYtMTA1M19sZW9wYXJkLmpwZyJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"e6795790-4a5f-4d47-ac93-de7d7713698b",
#          "related_to":"13632f77-6858-4721-8603-64138a9f38aa",
#          "stream_type":"att",
#          "source_id":"None",
#          "external_source_id":"None",
#          "destination_id":"338225f3-91f9-4fe1-b013-353a229ce504",
#          "data_provider_id":"ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "annotations":"null",
#          "tracing_context":"{}"
#        }
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# Gundi v1
#  -d '{
#        "message": {
#          "data":"eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
#          "attributes":{
#             "observation_type":"ps",
#             "device_id":"018910980",
#             "outbound_config_id":"1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
#             "integration_id":"36485b4f-88cd-49c4-a723-0ddff1f580c4",
#             "tracing_context":"{}"
#          }
#        },
#        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#      }'
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
