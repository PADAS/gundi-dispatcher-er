curl localhost:8080 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
      "message": {
        "data":"eyJldmVudF9pZCI6ICJhZTdmNjFkZi0yYjg1LTQ3MDMtOTVkYi0yYWUyNDI0YzBkZjYiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTQ6NTA6NTAuMDY1NjU0KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7InRpdGxlIjogIkFuaW1hbCBEZXRlY3RlZCBUZXN0IEV2ZW50IiwgImV2ZW50X3R5cGUiOiAibGlvbl9zaWdodGluZyIsICJ0aW1lIjogIjIwMjQtMDctMjQgMTQ6MTg6MTIrMDA6MDAiLCAibG9jYXRpb24iOiB7ImxvbmdpdHVkZSI6IDEzLjc4MzA2NSwgImxhdGl0dWRlIjogMTMuNjg4NjM1fSwgImV2ZW50X2RldGFpbHMiOiB7InNwZWNpZXMiOiAiTGlvbiJ9fSwgImV2ZW50X3R5cGUiOiAiRXZlbnRUcmFuc2Zvcm1lZEVSIn0=",
        "attributes":{
          "gundi_version":"v2",
          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
          "gundi_id":"6cb82182-51b2-4309-ba83-c99ed8e61ae8",
          "related_to":"None",
          "stream_type":"ev",
          "source_id":"ac1b9cdc-a193-4515-b446-b177bcc5f342",
          "external_source_id":"camera123",
          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
          "annotations":"{}",
          "tracing_context":"{}"
        },
        "messageId": "11937923011474843",
        "message_id": "11937923011474843",
        "publishTime": "2024-07-24T12:51:00.789Z",
        "publish_time": "2024-07-24T12:51:00.789Z"
      },
      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
    }'


# ObservationTransformedER v2 [Ok]
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICI0OGJkMDczYS04ZTM1LTQzY2YtOTFjMi1jN2I0Yjg3YTI2ZDciLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTM6MjM6NDMuOTUyMDU2KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Im1hbnVmYWN0dXJlcl9pZCI6ICJ0ZXN0LWRldmljZSIsICJzb3VyY2VfdHlwZSI6ICJ0cmFja2luZy1kZXZpY2UiLCAic3ViamVjdF9uYW1lIjogIk1hcmlhbm8iLCAic3ViamVjdF90eXBlIjogIm1tLXRyYWNrZXIiLCAic3ViamVjdF9zdWJ0eXBlIjogIm1tLXRyYWNrZXIiLCAicmVjb3JkZWRfYXQiOiAiMjAyNC0wNy0yMiAxMTo1MTowNSswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogLTcyLjcwNDQ1OSwgImxhdCI6IC01MS42ODgyNDZ9LCAiYWRkaXRpb25hbCI6IHsic3BlZWRfa21waCI6IDMwfX0sICJldmVudF90eXBlIjogIk9ic2VydmF0aW9uVHJhbnNmb3JtZWRFUiJ9",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"29074f3b-bf2f-43fa-8091-6943039935d1",
#          "related_to": "None",
#          "stream_type":"obv",
#          "source_id":"eb47e6ad-a677-4218-856b-59ad4d8d0e73",
#          "external_source_id":"test-device",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
#
# EventTransformedER v2 [Ok]
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICJhZTdmNjFkZi0yYjg1LTQ3MDMtOTVkYi0yYWUyNDI0YzBkZjYiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTQ6NTA6NTAuMDY1NjU0KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7InRpdGxlIjogIkFuaW1hbCBEZXRlY3RlZCBUZXN0IEV2ZW50IiwgImV2ZW50X3R5cGUiOiAibGlvbl9zaWdodGluZyIsICJ0aW1lIjogIjIwMjQtMDctMjQgMTQ6MTg6MTIrMDA6MDAiLCAibG9jYXRpb24iOiB7ImxvbmdpdHVkZSI6IDEzLjc4MzA2NSwgImxhdGl0dWRlIjogMTMuNjg4NjM1fSwgImV2ZW50X2RldGFpbHMiOiB7InNwZWNpZXMiOiAiTGlvbiJ9fSwgImV2ZW50X3R5cGUiOiAiRXZlbnRUcmFuc2Zvcm1lZEVSIn0=",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"6cb82182-51b2-4309-ba83-c99ed8e61ae8",
#          "related_to":"None",
#          "stream_type":"ev",
#          "source_id":"ac1b9cdc-a193-4515-b446-b177bcc5f342",
#          "external_source_id":"camera123",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{\"x-cloud-trace-context\": \"95f36c1f22b1cb599efc28243a631f7d/15139689239813763386;o=1\"}"
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# EventUpdateTransformedER v2 [Ok]
#  -d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICI2MzIyNjI2YS01YzQxLTQ4NmItOWE4YS04ZWZmODhhMDEyMjEiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTI6MDE6MDQuOTcxMjQwKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7ImNoYW5nZXMiOiB7ImV2ZW50X3R5cGUiOiJsaW9uX3NpZ2h0aW5nX3JlcCIsICJldmVudF9kZXRhaWxzIjogeyJzcGVjaWVzIjogIkxpb24iLCAicXVhbnRpdHkiOiAxfX19LCAiZXZlbnRfdHlwZSI6ICJFdmVudFVwZGF0ZVRyYW5zZm9ybWVkRVIifQ==",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "gundi_id":"6cb82182-51b2-4309-ba83-c99ed8e61ae8",
#          "related_to":"None",
#          "stream_type":"evu",
#          "source_id":"ac1b9cdc-a193-4515-b446-b177bcc5f342",
#          "external_source_id":"camera123",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{\"x-cloud-trace-context\": \"95f36c1f22b1cb599efc28243a631f7d/15139689239813763386;o=1\"}"
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "orderingKey": "b9ddcc3e-851a-4ec8-a1f4-4da1a5644ffb",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
# AttachmentTransformedER v2 [Ok]
#-d '{
#      "message": {
#        "data":"eyJldmVudF9pZCI6ICI5NjNkYmM1Ni03ZWVhLTQ5NDktYjM0ZS1hMWMwNWRhYWNjNGUiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMjA6Mzg6MDQuNDA0NzM5KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7ImZpbGVfcGF0aCI6ICJhdHRhY2htZW50cy85YmVkYzAzZS04NDE1LTQ2ZGItYWE3MC03ODI0OTBjZGZmMzFfd2lsZF9kb2ctbWFsZS5zdmcifSwgImV2ZW50X3R5cGUiOiAiQXR0YWNobWVudFRyYW5zZm9ybWVkRVIifQ==",
#        "attributes":{
#          "gundi_version":"v2",
#          "provider_key":"gundi_traptagger_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
#          "gundi_id":"9bedc03e-8415-46db-aa70-782490cdff31",
#          "related_to": "6cb82182-51b2-4309-ba83-c99ed8e61ae8",
#          "stream_type":"att",
#          "source_id":"eb47e6ad-a677-4218-856b-59ad4d8d0e73",
#          "external_source_id":"test-device",
#          "destination_id":"f45b1d48-46fc-414b-b1ca-7b56b87b2020",
#          "data_provider_id":"d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
#          "annotations":"{}",
#          "tracing_context":"{}"
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
#
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
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
#      },
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
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
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
#        },
#        "messageId": "11937923011474843",
#        "message_id": "11937923011474843",
#        "publishTime": "2024-07-24T12:51:00.789Z",
#        "publish_time": "2024-07-24T12:51:00.789Z"
#      },
#      "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#    }'
