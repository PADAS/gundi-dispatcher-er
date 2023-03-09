import datetime
import pytest
import asyncio
from gundi_client.schemas import OutboundConfiguration
from functions_framework.event_conversion import CloudEvent


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_cache(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    return mock_cache


@pytest.fixture
def mock_gundi_client(
        mocker,
        inbound_integration_config,
        outbound_integration_config,
        outbound_integration_config_list,
        device,
):
    mock_client = mocker.MagicMock()
    mock_client.get_inbound_integration.return_value = async_return(
        inbound_integration_config
    )
    mock_client.get_outbound_integration.return_value = async_return(
        outbound_integration_config
    )
    mock_client.get_outbound_integration_list.return_value = async_return(
        outbound_integration_config_list
    )
    mock_client.ensure_device.return_value = async_return(device)
    return mock_client


@pytest.fixture
def mock_pubsub_client(mocker, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_client.PublisherClient.return_value = mock_publisher
    return mock_client


@pytest.fixture
def mock_erclient_class(mocker, post_sensor_observation_response):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.post_sensor_observation.return_value = async_return(
        post_sensor_observation_response
    )
    erclient_mock.post_report.return_value = async_return(
        post_report_response
    )
    erclient_mock.post_camera_trap_report.return_value = async_return(
        post_camera_trap_report_response
    )
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_get_cloud_storage(mocker):
    return mocker.MagicMock()


@pytest.fixture
def post_sensor_observation_response():
    return {}


@pytest.fixture
def post_report_response():
    return {'id': '7f42ab47-fa7a-4a7e-acc6-cadcaa114646', 'location': {'latitude': 20.806785, 'longitude': -55.78498},
            'time': '2023-03-07T10:24:02-08:00', 'end_time': None, 'serial_number': 29260, 'message': '',
            'provenance': '', 'event_type': 'rainfall_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
            'comment': None, 'title': 'Rainfall', 'notes': [], 'reported_by': None, 'state': 'new', 'event_details': {},
            'contains': [], 'is_linked_to': [], 'is_contained_in': [], 'files': [], 'related_subjects': [],
            'sort_at': '2023-03-09T04:36:59.405991-08:00', 'patrol_segments': [], 'geometry': None,
            'updated_at': '2023-03-09T04:36:59.405991-08:00', 'created_at': '2023-03-09T04:36:59.406835-08:00',
            'icon_id': 'rainfall_rep', 'event_category': 'monitoring',
            'url': 'https://gundi-load-testing.pamdas.org/api/v1.0/activity/event/7f42ab47-fa7a-4a7e-acc6-cadcaa114646',
            'image_url': 'https://gundi-load-testing.pamdas.org/static/sprite-src/rainfall_rep.svg',
            'geojson': {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [-55.78498, 20.806785]},
                        'properties': {'message': '', 'datetime': '2023-03-07T18:24:02+00:00',
                                       'image': 'https://gundi-load-testing.pamdas.org/static/sprite-src/rainfall_rep.svg',
                                       'icon': {
                                           'iconUrl': 'https://gundi-load-testing.pamdas.org/static/sprite-src/rainfall_rep.svg',
                                           'iconSize': [25, 25], 'iconAncor': [12, 12], 'popupAncor': [0, -13],
                                           'className': 'dot'}}}, 'is_collection': False, 'updates': [
            {'message': 'Added', 'time': '2023-03-09T12:36:59.466462+00:00',
             'user': {'username': 'gundi_serviceaccount', 'first_name': 'Gundi', 'last_name': 'Service Account',
                      'id': '408388d0-bb42-43f2-a2c3-6805bcb5f315', 'content_type': 'accounts.user'},
             'type': 'add_event'}], 'patrols': []}


@pytest.fixture
def post_camera_trap_report_response():
    return {}


@pytest.fixture
def er_client_close_response():
    return {}


@pytest.fixture
def gcp_pubsub_publish_response():
    return {"messageIds": ["7061707768812258"]}


@pytest.fixture
def inbound_integration_config():
    return {
        "state": {},
        "id": "12345b4f-88cd-49c4-a723-0ddff1f580c4",
        "type": "1234e5bd-a473-4c02-9227-27b6134615a4",
        "owner": "1234191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "endpoint": "https://logins.testbidtrack.co.za/restintegration/",
        "login": "test",
        "password": "test",
        "token": "",
        "type_slug": "bidtrack",
        "provider": "bidtrack",
        "default_devicegroup": "1234cfdc-1aae-44b0-8e0a-22c72355ea85",
        "enabled": True,
        "name": "BidTrack - Manyoni",
    }


@pytest.fixture
def outbound_integration_config():
    return {
        "id": "2222dc7e-73e2-4af3-93f5-a1cb322e5add",
        "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
        "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "name": "[Internal] AI2 Test -  Bidtrack to  ER",
        "endpoint": "https://cdip-er.pamdas.org/api/v1.0",
        "state": {},
        "login": "",
        "password": "",
        "token": "1111d87681cd1d01ad07c2d0f57d15d6079ae7d7",
        "type_slug": "earth_ranger",
        "inbound_type_slug": "bidtrack",
        "additional": {},
    }


@pytest.fixture
def outbound_integration_config_list():
    return [
        {
            "id": "2222dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER",
            "endpoint": "https://cdip-er.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "1111d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {},
        }
    ]


@pytest.fixture
def device():
    return {
        "id": "564daff9-8cac-4004-b1a4-e169cf3b4bdb",
        "external_id": "018910999",
        "name": "",
        "subject_type": None,
        "inbound_configuration": {
            "state": {},
            "id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
            "type": "b069e5bd-a473-4c02-9227-27b6134615a4",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "endpoint": "https://logins.bidtrack.co.za/restintegration/",
            "login": "ZULULANDRR",
            "password": "Rh1n0#@!",
            "token": "",
            "type_slug": "bidtrack",
            "provider": "bidtrack",
            "default_devicegroup": "0da5cfdc-1aae-44b0-8e0a-22c72355ea85",
            "enabled": True,
            "name": "BidTrack - Manyoni",
        },
        "additional": {},
    }


@pytest.fixture
def unprocessed_observation_position():
    return b'{"attributes": {"observation_type": "ps"}, "data": {"id": null, "owner": "na", "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4", "device_id": "018910980", "name": "Logistics Truck test", "type": "tracking-device", "subject_type": null, "recorded_at": "2023-03-03 09:34:00+02:00", "location": {"x": 35.43935, "y": -1.59083, "z": 0.0, "hdop": null, "vdop": null}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}, "voltage": null, "temperature": null, "radio_status": null, "observation_type": "ps"}}'


@pytest.fixture
def transformed_observation_position():
    return {
        "manufacturer_id": "018910980",
        "source_type": "tracking-device",
        "subject_name": "Logistics Truck test",
        "recorded_at": datetime.datetime(
            2023,
            3,
            2,
            18,
            47,
            tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)),
        ),
        "location": {"lon": 35.43929, "lat": -1.59083},
        "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"},
    }


@pytest.fixture
def transformed_observation_attributes():
    return {
        "observation_type": "ps",
        "device_id": "018910980",
        "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
    }


@pytest.fixture
def transformed_observation_gcp_message():
    return b'{"manufacturer_id": "018910980", "source_type": "tracking-device", "subject_name": "Logistics Truck test", "recorded_at": "2023-03-02 18:47:00+02:00", "location": {"lon": 35.43929, "lat": -1.59083}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}}'


@pytest.fixture
def outbound_configuration_gcp_pubsub():
    return OutboundConfiguration.parse_obj(
        {
            "id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER load test",
            "endpoint": "https://gundi-load-testing.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "0890d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {"broker": "gcp_pubsub"},
        }
    )


@pytest.fixture
def position_as_cloud_event():
    return CloudEvent(
        attributes={
            'specversion': '1.0',
            'id': '123451234512345',
            'source': '//pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC',
            'type': 'google.cloud.pubsub.topic.v1.messagePublished', 'datacontenttype': 'application/json',
            'time': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        },
        data={
            "message": {
                "data": "eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
                "attributes": {
                    "observation_type": "ps",
                    "device_id": "018910980",
                    "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
                    "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
                    "tracing_context": "{}"
                }
            },
            "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
        }
    )


@pytest.fixture
def geoevent_as_cloud_event():
    return CloudEvent(
        attributes={
            'specversion': '1.0', 'id': '123451234512345',
            'source': '//pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC',
            'type': 'google.cloud.pubsub.topic.v1.messagePublished', 'datacontenttype': 'application/json',
            'time': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        },
        data={
            'message': {
                'data': 'eyJ0aXRsZSI6ICJSYWluZmFsbCIsICJldmVudF90eXBlIjogInJhaW5mYWxsX3JlcCIsICJldmVudF9kZXRhaWxzIjogeyJhbW91bnRfbW0iOiA2LCAiaGVpZ2h0X20iOiAzfSwgInRpbWUiOiAiMjAyMy0wMy0wNyAxMToyNDowMi0wNzowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogLTU1Ljc4NDk4LCAibGF0aXR1ZGUiOiAyMC44MDY3ODV9fQ==',
                'attributes': {
                    'observation_type': 'ge', 'device_id': '003',
                    'outbound_config_id': '9243a5e3-b16a-4dbd-ad32-197c58aeef59',
                    'integration_id': '8311c4a5-ddab-4743-b8ab-d3d57a7c8212', 'tracing_context': '{}'
                }
            },
            'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
        }
    )


@pytest.fixture
def cameratrap_event_as_cloud_event():
    return CloudEvent(
        attributes={
            'specversion': '1.0', 'id': '123451234512345',
            'source': '//pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC',
            'type': 'google.cloud.pubsub.topic.v1.messagePublished', 'datacontenttype': 'application/json',
            'time': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        },
        data={
            'message': {
                'data': 'eyJmaWxlIjogImNhbWVyYXRyYXAuanBnIiwgImNhbWVyYV9uYW1lIjogIk1hcmlhbm8ncyBDYW1lcmEiLCAiY2FtZXJhX2Rlc2NyaXB0aW9uIjogInRlc3QgY2FtZXJhIiwgInRpbWUiOiAiMjAyMy0wMy0wNyAxMTo1MTowMC0wMzowMCIsICJsb2NhdGlvbiI6ICJ7XCJsb25naXR1ZGVcIjogLTEyMi41LCBcImxhdGl0dWRlXCI6IDQ4LjY1fSJ9',
                'attributes': {
                    'observation_type': 'ct', 'device_id': 'Mariano Camera',
                    'outbound_config_id': '5f658487-67f7-43f1-8896-d78778e49c30',
                    'integration_id': 'a244fddd-3f64-4298-81ed-b6fccc60cef8', 'tracing_context': '{}'
                }
            },
            'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
        }
    )
