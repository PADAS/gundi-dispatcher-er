import datetime
import json
import httpx
import pytest
import asyncio
from erclient import ERClientServiceUnavailable
from gundi_core.schemas import OutboundConfiguration
from redis import exceptions as redis_exceptions
import gundi_core.schemas.v2 as schemas_v2
from gundi_core import events as system_events
from gcloud.aio import pubsub
from core import settings


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_cache_empty(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    return mock_cache


@pytest.fixture
def dispatched_event():
    return schemas_v2.DispatchedObservation(
        gundi_id="23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
        related_to=None,
        external_id="ABC123",  # ID returned by the destination system
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        destination_id="338225f3-91f9-4fe1-b013-353a229ce504",
        delivered_at=datetime.datetime.now()  # UTC
    )


@pytest.fixture
def dispatched_event_trace(dispatched_event):
    return schemas_v2.GundiTrace(
        object_id=str(dispatched_event.gundi_id),
        object_type='ev',
        related_to=None,
        data_provider=str(dispatched_event.data_provider_id),
        destination=str(dispatched_event.destination_id),
        delivered_at=dispatched_event.delivered_at,
        external_id=dispatched_event.external_id,
        created_at=datetime.datetime.now(tz=datetime.timezone.utc),
        updated_at=None,
        last_update_delivered_at=None,
        is_duplicate=False,
        has_error=False
    )


@pytest.fixture
def mock_cache_with_one_miss_then_hit(mocker, dispatched_event):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, dispatched_event.json())
    return mock_cache


@pytest.fixture
def mock_cache_with_cached_event(mocker, dispatched_event):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = dispatched_event.json()
    return mock_cache


@pytest.fixture
def mock_cache_with_connection_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = redis_exceptions.ConnectionError(
        "Error while reading from 172.22.161.3:6379 : (104, 'Connection reset by peer')"
    )
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
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_class(mocker, mock_gundi_client):
    mock_gundi_client_class = mocker.MagicMock()
    mock_gundi_client_class.return_value = mock_gundi_client
    return mock_gundi_client_class


@pytest.fixture
def mock_gundi_client_with_client_connect_error(
        mocker,
        inbound_integration_config,
        outbound_integration_config,
        outbound_integration_config_list,
        device,
):
    mock_client = mocker.MagicMock()
    # Simulate a connection error
    client_connector_error = httpx.ConnectError("Connection error")
    # Side effects to raise an exception when a method is called
    mock_client.get_inbound_integration.side_effect = client_connector_error
    mock_client.get_outbound_integration.side_effect = client_connector_error
    mock_client.get_outbound_integration_list.side_effect = client_connector_error
    mock_client.ensure_device.side_effect = client_connector_error
    return mock_client


@pytest.fixture
def mock_gundi_client_class_with_client_connect_error(mocker, mock_gundi_client_with_client_connect_error):
    mock_gundi_client_class_with_error = mocker.MagicMock()
    mock_gundi_client_class_with_error.return_value = mock_gundi_client_with_client_connect_error
    return mock_gundi_client_class_with_error


# ToDo: parametrize with different status codes?
@pytest.fixture
def mock_gundi_client_with_500_error(
        mocker,
        inbound_integration_config,
        outbound_integration_config,
        outbound_integration_config_list,
        device,
):
    mock_client = mocker.MagicMock()
    # Simulate a connection error
    client_connector_error = httpx.Response(status_code=500, text="Internal Server Error")
    # Side effects to raise an exception when a method is called
    mock_client.get_inbound_integration.side_effect = client_connector_error
    mock_client.get_outbound_integration.side_effect = client_connector_error
    mock_client.get_outbound_integration_list.side_effect = client_connector_error
    mock_client.ensure_device.side_effect = client_connector_error
    return mock_client


@pytest.fixture
def mock_gundi_client_with_internal_exception(
        mocker,
        inbound_integration_config,
        outbound_integration_config,
        outbound_integration_config_list,
        device,
):
    mock_client = mocker.MagicMock()
    # Simulate an unhandled exception
    internal_exception = Exception("The gundi client has a bug!")
    # Side effects to raise an exception when a method is called
    mock_client.get_inbound_integration.side_effect = internal_exception
    mock_client.get_outbound_integration.side_effect = internal_exception
    mock_client.get_outbound_integration_list.side_effect = internal_exception
    mock_client.ensure_device.side_effect = internal_exception
    return mock_client


@pytest.fixture
def mock_gundi_client_class_with_with_500_error(mocker, mock_gundi_client_with_500_error):
    mock_gundi_client_class_with_error = mocker.MagicMock()
    mock_gundi_client_class_with_error.return_value = mock_gundi_client_with_500_error
    return mock_gundi_client_class_with_error


@pytest.fixture
def mock_gundi_client_class_with_internal_exception(mocker, mock_gundi_client_with_internal_exception):
    mock_gundi_client_class_with_error = mocker.MagicMock()
    mock_gundi_client_class_with_error.return_value = mock_gundi_client_with_internal_exception
    return mock_gundi_client_class_with_error


@pytest.fixture
def observation_delivered_pubsub_message():
    return pubsub.PubsubMessage(
        b'{"event_id": "c05cf942-f543-4798-bd91-0e38a63d655e", "timestamp": "2023-07-12 20:34:07.210731+00:00", "schema_version": "v1", "payload": {"gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638", "related_to": "None", "external_id": "7f42ab47-fa7a-4a7e-acc6-cadcaa114646", "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6", "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504", "delivered_at": "2023-07-12 20:34:07.210542+00:00"}, "event_type": "ObservationDelivered"}'
    )


@pytest.fixture
def mock_pubsub_client(mocker, observation_delivered_pubsub_message, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_publisher.topic_path.return_value = f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}"
    mock_client.PublisherClient.return_value = mock_publisher
    mock_client.PubsubMessage.return_value = observation_delivered_pubsub_message
    return mock_client


@pytest.fixture
def observation_delivery_failure_pubsub_message():
    return pubsub.PubsubMessage(
        b'{"event_id": "a13c5742-8199-404b-a41b-f520d7462d74", "timestamp": "2023-07-13 14:17:13.323863+00:00", "schema_version": "v1", "payload": {"gundi_id": "9f86ae28-99c4-473f-be13-fb92bd0bc341", "related_to": null, "external_id": null, "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6", "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504", "delivered_at": "2023-07-13 14:17:13.323706+00:00"}, "event_type": "ObservationDeliveryFailed"}')


@pytest.fixture
def mock_pubsub_client_with_observation_delivery_failure(mocker, observation_delivery_failure_pubsub_message,
                                                         gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_publisher.topic_path.return_value = f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}"
    mock_client.PublisherClient.return_value = mock_publisher
    mock_client.PubsubMessage.return_value = observation_delivery_failure_pubsub_message
    return mock_client


@pytest.fixture
def mock_erclient_class(
        mocker,
        post_sensor_observation_response,
        post_report_response,
        patch_report_reponse,
        post_report_attachment_response,
        post_camera_trap_report_response,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.post_sensor_observation.return_value = async_return(
        post_sensor_observation_response
    )
    erclient_mock.post_report.return_value = async_return(
        post_report_response
    )
    erclient_mock.patch_report.return_value = async_return(
        patch_report_reponse
    )
    erclient_mock.post_report_attachment.return_value = async_return(
        post_report_attachment_response
    )
    erclient_mock.post_camera_trap_report.return_value = async_return(
        post_camera_trap_report_response
    )
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_erclient_class_with_service_unavailable_error(
        mocker,
        post_sensor_observation_response,
        post_report_response,
        post_report_attachment_response,
        post_camera_trap_report_response,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    error = ERClientServiceUnavailable('ER service unavailable')
    erclient_mock.post_sensor_observation.side_effect = error
    erclient_mock.post_report.side_effect = error
    erclient_mock.post_report_attachment.side_effect = error
    erclient_mock.post_camera_trap_report.side_effect = error
    erclient_mock.close.side_effect = error
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
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
def patch_report_reponse():
    return {}


@pytest.fixture
def post_report_attachment_response():
    return {}


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
def position_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            "data": "eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
            "attributes": {
                "observation_type": "ps",
                "device_id": "018910980",
                "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
                "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def position_as_request_with_future_timestamp(mocker):
    future_datetime = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
    future_timestamp = future_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    json_data = {
        'message': {
            "data": "eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
            "attributes": {
                "observation_type": "ps",
                "device_id": "018910980",
                "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
                "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "publishTime": f"{future_timestamp}",
            "publish_time": f"{future_timestamp}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def position_as_request_with_old_timestamp(mocker):
    old_datetime = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=25)
    old_timestamp = old_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    json_data = {
        'message': {
            "data": "eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
            "attributes": {
                "observation_type": "ps",
                "device_id": "018910980",
                "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
                "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "publishTime": f"{old_timestamp}",
            "publish_time": f"{old_timestamp}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def geoevent_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        "message": {
            "data": 'eyJ0aXRsZSI6ICJSYWluZmFsbCIsICJldmVudF90eXBlIjogInJhaW5mYWxsX3JlcCIsICJldmVudF9kZXRhaWxzIjogeyJhbW91bnRfbW0iOiA2LCAiaGVpZ2h0X20iOiAzfSwgInRpbWUiOiAiMjAyMy0wMy0wNyAxMToyNDowMi0wNzowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogLTU1Ljc4NDk4LCAibGF0aXR1ZGUiOiAyMC44MDY3ODV9fQ==',
            "attributes": {
                "observation_type": "ge",
                "device_id": "003",
                "outbound_config_id": "9243a5e3-b16a-4dbd-ad32-197c58aeef59",
                "integration_id": "8311c4a5-ddab-4743-b8ab-d3d57a7c8212",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def cameratrap_event_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        "message": {
            "data": 'eyJmaWxlIjogImNhbWVyYXRyYXAuanBnIiwgImNhbWVyYV9uYW1lIjogIk1hcmlhbm8ncyBDYW1lcmEiLCAiY2FtZXJhX2Rlc2NyaXB0aW9uIjogInRlc3QgY2FtZXJhIiwgInRpbWUiOiAiMjAyMy0wMy0wNyAxMTo1MTowMC0wMzowMCIsICJsb2NhdGlvbiI6ICJ7XCJsb25naXR1ZGVcIjogLTEyMi41LCBcImxhdGl0dWRlXCI6IDQ4LjY1fSJ9',
            "attributes": {
                "observation_type": "ct",
                "device_id": "Mariano Camera",
                "outbound_config_id": "5f658487-67f7-43f1-8896-d78778e49c30",
                "integration_id": "a244fddd-3f64-4298-81ed-b6fccc60cef8",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request



@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        destination_integration_v2,
        dispatched_event_trace
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        destination_integration_v2
    )
    mock_client.get_traces.return_value = async_return([dispatched_event_trace])
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_with_internal_exception(mocker):
    mock_client = mocker.MagicMock()
    internal_exception = Exception("The gundi client has a bug!")
    # Side effects to raise an exception when a method is called
    mock_client.get_integration_details.side_effect = internal_exception
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def mock_gundi_client_v2_class_with_internal_exception(mocker, mock_gundi_client_v2_with_internal_exception):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2_with_internal_exception
    return mock_gundi_client_v2_class


@pytest.fixture
def destination_integration_v2():
    return schemas_v2.Integration.parse_obj(
        {'id': '338225f3-91f9-4fe1-b013-353a229ce504', 'name': 'ER Load Testing',
         'base_url': 'https://gundi-load-testing.pamdas.org', 'enabled': True,
         'type': {'id': '45c66a61-71e4-4664-a7f2-30d465f87aa6', 'name': 'EarthRanger', 'value': 'earth_ranger',
                  'description': 'Integration type for Earth Ranger Sites', 'actions': [
                 {'id': '43ec4163-2f40-43fc-af62-bca1db77c06b', 'type': 'auth', 'name': 'Authenticate', 'value': 'auth',
                  'description': 'Authenticate against Earth Ranger',
                  'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                 {'id': '036c2098-f494-40ec-a595-710b314d5ea5', 'type': 'pull', 'name': 'Pull Positions',
                  'value': 'pull_positions', 'description': 'Pull position data from an Earth Ranger site',
                  'schema': {'type': 'object', 'required': ['endpoint'],
                             'properties': {'endpoint': {'type': 'string'}}}},
                 {'id': '9286bb71-9aca-425a-881f-7fe0b2dba4f4', 'type': 'push', 'name': 'Push Events',
                  'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                  'schema': {}},
                 {'id': 'aae0cf50-fbc7-4810-84fd-53fb75020a43', 'type': 'push', 'name': 'Push Positions',
                  'value': 'push_positions', 'description': 'Push position data to an Earth Ranger site',
                  'schema': {'type': 'object', 'required': ['endpoint'],
                             'properties': {'endpoint': {'type': 'string'}}}}]},
         'owner': {'id': 'e2d1b0fc-69fe-408b-afc5-7f54872730c0', 'name': 'Test Organization', 'description': ''},
         'configurations': [
             {'id': '013ea7ce-4944-4f7e-8a2f-e5338b3741ce', 'integration': '338225f3-91f9-4fe1-b013-353a229ce504',
              'action': {'id': '43ec4163-2f40-43fc-af62-bca1db77c06b', 'type': 'auth', 'name': 'Authenticate',
                         'value': 'auth'}, 'data': {'token': '1190d87681cd1d01ad07c2d0f57d15d6079ae7ab'}},
             {'id': '5de91c7b-f28a-4ce7-8137-273ac10674d2', 'integration': '338225f3-91f9-4fe1-b013-353a229ce504',
              'action': {'id': 'aae0cf50-fbc7-4810-84fd-53fb75020a43', 'type': 'push', 'name': 'Push Positions',
                         'value': 'push_positions'}, 'data': {'endpoint': 'api/v1/positions'}},
             {'id': '7947b19e-1d2d-4ca3-bd6c-74976ae1de68', 'integration': '338225f3-91f9-4fe1-b013-353a229ce504',
              'action': {'id': '036c2098-f494-40ec-a595-710b314d5ea5', 'type': 'pull', 'name': 'Pull Positions',
                         'value': 'pull_positions'}, 'data': {'endpoint': 'api/v1/positions'}}],
         'additional': {'topic': 'destination-v2-338225f3-91f9-4fe1-b013-353a229ce504-dev', 'broker': 'gcp_pubsub'},
         'default_route': {'id': '38dd8ec2-b3ee-4c31-940e-b6cc9c1f4326', 'name': 'Mukutan - Load Testing'},
         'status': {'id': 'mockid-b16a-4dbd-ad32-197c58aeef59', 'is_healthy': True,
                    'details': 'Last observation has been delivered with success.',
                    'observation_delivered_24hrs': 50231, 'last_observation_delivered_at': '2023-03-31T11:20:00+0200'}
         }
    )


@pytest.fixture
def event_v2_as_pubsub_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI4NzdmNmQ1Ni05ZDAyLTQzODctODI2ZS0xZTc3ZWMyZjg5YjYiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjMgMTk6NTQ6MDAuNjU5OTYzKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7InRpdGxlIjogIkFuaW1hbCBEZXRlY3RlZCBUZXN0IEV2ZW50IiwgImV2ZW50X3R5cGUiOiAid2lsZGxpZmVfc2lnaHRpbmdfcmVwIiwgInRpbWUiOiAiMjAyNC0wNy0wNCAxODowOToxMiswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogMTMuNzgzMDY0LCAibGF0aXR1ZGUiOiAxMy42ODg2MzV9LCAiZXZlbnRfZGV0YWlscyI6IHsic3BlY2llcyI6ICJsaW9uIn19LCAiZXZlbnRfdHlwZSI6ICJFdmVudFRyYW5zZm9ybWVkRVIifQ==',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "awt",
                "gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "related_to": "None",
                "stream_type": "ev",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "orderingKey": "",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def event_v2_with_state_as_pubsub_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI4NzdmNmQ1Ni05ZDAyLTQzODctODI2ZS0xZTc3ZWMyZjg5YjYiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjMgMTk6NTQ6MDAuNjU5OTYzKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7InRpdGxlIjogIkFuaW1hbCBEZXRlY3RlZCBUZXN0IEV2ZW50IiwgImV2ZW50X3R5cGUiOiAid2lsZGxpZmVfc2lnaHRpbmdfcmVwIiwgInRpbWUiOiAiMjAyNC0wNy0wNCAxODowOToxMiswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogMTMuNzgzMDY0LCAibGF0aXR1ZGUiOiAxMy42ODg2MzV9LCAiZXZlbnRfZGV0YWlscyI6IHsic3BlY2llcyI6ICJsaW9uIn0sICJzdGF0ZSI6ICJhY3RpdmUifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRUcmFuc2Zvcm1lZEVSIn0=',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "awt_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "related_to": "None",
                "stream_type": "ev",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "orderingKey": "",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def event_v2_as_eventarc_request(mocker):
    mock_request = mocker.MagicMock()
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    mock_request.headers = {
        "Content-Type": "application/json",
        "ce-id": "123451234512345",
        "ce-specversion": "1.0",
        "ce-time": publish_time,
        "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
        "ce-source": "//pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC"
    }
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI4NzdmNmQ1Ni05ZDAyLTQzODctODI2ZS0xZTc3ZWMyZjg5YjYiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjMgMTk6NTQ6MDAuNjU5OTYzKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7InRpdGxlIjogIkFuaW1hbCBEZXRlY3RlZCBUZXN0IEV2ZW50IiwgImV2ZW50X3R5cGUiOiAid2lsZGxpZmVfc2lnaHRpbmdfcmVwIiwgInRpbWUiOiAiMjAyNC0wNy0wNCAxODowOToxMiswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogMTMuNzgzMDY0LCAibGF0aXR1ZGUiOiAxMy42ODg2MzV9LCAiZXZlbnRfZGV0YWlscyI6IHsic3BlY2llcyI6ICJsaW9uIn0sICJzdGF0ZSI6ICJhY3RpdmUifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRUcmFuc2Zvcm1lZEVSIn0=',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "awt_ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "related_to": "None",
                "stream_type": "ev",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            }
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def event_update_v2_as_pubsub_request(mocker, dispatched_event_trace):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            "data": "eyJldmVudF9pZCI6ICI2MzIyNjI2YS01YzQxLTQ4NmItOWE4YS04ZWZmODhhMDEyMjEiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTI6MDE6MDQuOTcxMjQwKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7ImNoYW5nZXMiOiB7ImV2ZW50X3R5cGUiOiJsaW9uX3NpZ2h0aW5nX3JlcCIsICJldmVudF9kZXRhaWxzIjogeyJzcGVjaWVzIjogIkxpb24iLCAicXVhbnRpdHkiOiAxfX19LCAiZXZlbnRfdHlwZSI6ICJFdmVudFVwZGF0ZVRyYW5zZm9ybWVkRVIifQ==",
            "attributes": {
                "gundi_version": "v2",
                "provider_key": "gundi_traptagger_d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
                "gundi_id": str(dispatched_event_trace.object_id),
                "related_to": "None",
                "stream_type": "evu",
                "source_id": "ac1b9cdc-a193-4515-b446-b177bcc5f342",
                "external_source_id": "camera123",
                "destination_id": str(dispatched_event_trace.destination),
                "data_provider_id": str(dispatched_event_trace.data_provider),
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474843",
            "message_id": "11937923011474843",
            "orderingKey": "b9ddcc3e-851a-4ec8-a1f4-4da1a5644ffb",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def event_update_v2_as_eventarc_event(mocker, dispatched_event_trace):
    mock_request = mocker.MagicMock()
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    mock_request.headers = {
        "Content-Type": "application/json",
        "ce-id": "123451234512345",
        "ce-specversion": "1.0",
        "ce-time": publish_time,
        "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
        "ce-source": "//pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC"
    }
    json_data = {
        'message': {
            "data": "eyJldmVudF9pZCI6ICI2MzIyNjI2YS01YzQxLTQ4NmItOWE4YS04ZWZmODhhMDEyMjEiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTI6MDE6MDQuOTcxMjQwKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7ImNoYW5nZXMiOiB7ImV2ZW50X3R5cGUiOiJsaW9uX3NpZ2h0aW5nX3JlcCIsICJldmVudF9kZXRhaWxzIjogeyJzcGVjaWVzIjogIkxpb24iLCAicXVhbnRpdHkiOiAxfX19LCAiZXZlbnRfdHlwZSI6ICJFdmVudFVwZGF0ZVRyYW5zZm9ybWVkRVIifQ==",
            "attributes": {
                "gundi_version": "v2",
                "provider_key": "gundi_traptagger_d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
                "gundi_id": str(dispatched_event_trace.object_id),
                "related_to": "None",
                "stream_type": "evu",
                "source_id": "ac1b9cdc-a193-4515-b446-b177bcc5f342",
                "external_source_id": "camera123",
                "destination_id": str(dispatched_event_trace.destination),
                "data_provider_id": str(dispatched_event_trace.data_provider),
                "annotations": "{}",
                "tracing_context": "{}"
            }
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def event_update_v2_with_state_as_pubsub_request(mocker, dispatched_event_trace):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            "data": "eyJldmVudF9pZCI6ICI2MzIyNjI2YS01YzQxLTQ4NmItOWE4YS04ZWZmODhhMDEyMjEiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTI6MDE6MDQuOTcxMjQwKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7ImNoYW5nZXMiOiB7InN0YXRlIjogInJlc29sdmVkIn19LCAiZXZlbnRfdHlwZSI6ICJFdmVudFVwZGF0ZVRyYW5zZm9ybWVkRVIifQ==",
            "attributes": {
                "gundi_version": "v2",
                "provider_key": "gundi_traptagger_d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
                "gundi_id": str(dispatched_event_trace.object_id),
                "related_to": "None",
                "stream_type": "evu",
                "source_id": "ac1b9cdc-a193-4515-b446-b177bcc5f342",
                "external_source_id": "camera123",
                "destination_id": str(dispatched_event_trace.destination),
                "data_provider_id": str(dispatched_event_trace.data_provider),
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474850",
            "message_id": "11937923011474850",
            "orderingKey": "b9ddcc3e-851a-4ec8-a1f4-4da1a5644ffb",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def event_v2_with_provider_key_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI4NzdmNmQ1Ni05ZDAyLTQzODctODI2ZS0xZTc3ZWMyZjg5YjYiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjMgMTk6NTQ6MDAuNjU5OTYzKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7InRpdGxlIjogIkFuaW1hbCBEZXRlY3RlZCBUZXN0IEV2ZW50IiwgImV2ZW50X3R5cGUiOiAid2lsZGxpZmVfc2lnaHRpbmdfcmVwIiwgInRpbWUiOiAiMjAyNC0wNy0wNCAxODowOToxMiswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uZ2l0dWRlIjogMTMuNzgzMDY0LCAibGF0aXR1ZGUiOiAxMy42ODg2MzV9LCAiZXZlbnRfZGV0YWlscyI6IHsic3BlY2llcyI6ICJsaW9uIn0sICJnZW9tZXRyeSI6IHt9LCAicHJvdmlkZXJfa2V5IjogIm1hcGlwZWRpYSJ9LCAiZXZlbnRfdHlwZSI6ICJFdmVudFRyYW5zZm9ybWVkRVIifQ==',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "gundi_traptagger_f870e228-4a65-40f0-888c-41bdc1124c3c",
                "gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "related_to": "None",
                "stream_type": "ev",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474844",
            "message_id": "11937923011474844",
            "orderingKey": "",
            "publishTime": f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def observation_v2_with_provider_key_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI0OGJkMDczYS04ZTM1LTQzY2YtOTFjMi1jN2I0Yjg3YTI2ZDciLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTM6MjM6NDMuOTUyMDU2KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Im1hbnVmYWN0dXJlcl9pZCI6ICJ0ZXN0LWRldmljZSIsICJzb3VyY2VfdHlwZSI6ICJ0cmFja2luZy1kZXZpY2UiLCAic3ViamVjdF9uYW1lIjogIk1hcmlhbm8iLCAic3ViamVjdF90eXBlIjogIm1tLXRyYWNrZXIiLCAic3ViamVjdF9zdWJ0eXBlIjogIm1tLXRyYWNrZXIiLCAicmVjb3JkZWRfYXQiOiAiMjAyNC0wNy0yMiAxMTo1MTowNSswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogLTcyLjcwNDQ1OSwgImxhdCI6IC01MS42ODgyNDZ9LCAiYWRkaXRpb25hbCI6IHsic3BlZWRfa21waCI6IDMwfSwgInByb3ZpZGVyX2tleSI6ICJtYXBpcGVkaWEifSwgImV2ZW50X3R5cGUiOiAiT2JzZXJ2YXRpb25UcmFuc2Zvcm1lZEVSIn0=',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "gundi_cellstop_f870e228-4a65-40f0-888c-41bdc1124c3c",
                "gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "related_to": "None",
                "stream_type": "obv",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474845",
            "message_id": "11937923011474845",
            "orderingKey": "",
            "publishTime":  f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def observation_v2_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI0OGJkMDczYS04ZTM1LTQzY2YtOTFjMi1jN2I0Yjg3YTI2ZDciLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMTM6MjM6NDMuOTUyMDU2KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Im1hbnVmYWN0dXJlcl9pZCI6ICJ0ZXN0LWRldmljZSIsICJzb3VyY2VfdHlwZSI6ICJ0cmFja2luZy1kZXZpY2UiLCAic3ViamVjdF9uYW1lIjogIk1hcmlhbm8iLCAic3ViamVjdF90eXBlIjogIm1tLXRyYWNrZXIiLCAic3ViamVjdF9zdWJ0eXBlIjogIm1tLXRyYWNrZXIiLCAicmVjb3JkZWRfYXQiOiAiMjAyNC0wNy0yMiAxMTo1MTowNSswMDowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogLTcyLjcwNDQ1OSwgImxhdCI6IC01MS42ODgyNDZ9LCAiYWRkaXRpb25hbCI6IHsic3BlZWRfa21waCI6IDMwfX0sICJldmVudF90eXBlIjogIk9ic2VydmF0aW9uVHJhbnNmb3JtZWRFUiJ9',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "awt",
                "gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "related_to": "None",
                "stream_type": "obv",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474846",
            "message_id": "11937923011474846",
            "orderingKey": "",
            "publishTime":  f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def attachment_v2_as_request(mocker):
    mock_request = mocker.MagicMock()
    mock_request.headers = [("Host", "sandbox-earth-dis-762049ee-0613-466b-99a2-59107eb-jba4og2dyq-uc.a.run.app")]
    publish_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    json_data = {
        'message': {
            'data': 'eyJldmVudF9pZCI6ICI5NjNkYmM1Ni03ZWVhLTQ5NDktYjM0ZS1hMWMwNWRhYWNjNGUiLCAidGltZXN0YW1wIjogIjIwMjQtMDctMjQgMjA6Mzg6MDQuNDA0NzM5KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7ImZpbGVfcGF0aCI6ICJhdHRhY2htZW50cy85YmVkYzAzZS04NDE1LTQ2ZGItYWE3MC03ODI0OTBjZGZmMzFfd2lsZF9kb2ctbWFsZS5zdmcifSwgImV2ZW50X3R5cGUiOiAiQXR0YWNobWVudFRyYW5zZm9ybWVkRVIifQ==',
            'attributes': {
                "gundi_version": "v2",
                "provider_key": "awt",
                "gundi_id": "f1a8894b-ff2e-4286-90a0-8f17303e91df",
                "related_to": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
                "stream_type": "att",
                "source_id": "afa0d606-c143-4705-955d-68133645db6d",
                "external_source_id": "Xyz123",
                "destination_id": "338225f3-91f9-4fe1-b013-353a229ce504",
                "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "annotations": "{}",
                "tracing_context": "{}"
            },
            "messageId": "11937923011474847",
            "message_id": "11937923011474847",
            "orderingKey": "",
            "publishTime":  f"{publish_time}",
            "publish_time": f"{publish_time}"
        },
        'subscription': 'projects/MY-PROJECT/subscriptions/MY-SUB'
    }
    mock_request.data = json.dumps(json_data)
    mock_request.get_json.return_value = json_data
    return mock_request


@pytest.fixture
def dispatched_observation():
    return schemas_v2.DispatchedObservation(
        gundi_id="23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
        related_to=None,
        external_id="37314d00-731f-427c-aaa5-336daf13f904",  # ID returned by the destination system
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        destination_id="338225f3-91f9-4fe1-b013-353a229ce504",
        delivered_at=datetime.datetime.now(datetime.timezone.utc)
    )


@pytest.fixture
def observation_delivered_event(dispatched_observation):
    return system_events.ObservationDelivered(
        payload=dispatched_observation
    )
