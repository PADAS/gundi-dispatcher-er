import base64
import json
from unittest.mock import ANY, call
import pytest
from gundi_core.events import dispatchers as dispatcher_events
from erclient import er_errors
from core import settings
from core import dispatchers
from core.dispatchers import (
    EREventDispatcher,
    EREventAttachmentDispatcher,
    EREventUpdateDispatcher,
    ERObservationDispatcher,
)
from core.errors import DispatcherException
from core.event_handlers import dispatch_transformed_observation_v2


async def _test_dispatcher_on_errors(
        dispatcher_class,
        mocker,
        mock_cache_empty,
        mock_gundi_client_v2_class,
        mock_erclient_class_with_error,
        mock_get_cloud_storage,
        er_error,
        destination_integration_v2,
        observation,
        **kwargs
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class_with_error)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)

    # Check that the dispatcher raises an exception so the message is retried later
    with pytest.raises(er_error) as exc_info:
        dispatcher = dispatcher_class(integration=destination_integration_v2, provider="test")
        await dispatcher.send(observation, **kwargs)

    exception = exc_info.value
    expected_error = mock_erclient_class_with_error.return_value.post_report.side_effect
    assert hasattr(exception, "status_code")
    assert hasattr(exception, "response_body")
    assert exception == expected_error


@pytest.mark.parametrize(
    "dispatcher_class, mock_data, dispatcher_extra",
    [
        (EREventDispatcher, "event_v2_transformed_er", None),
        (EREventAttachmentDispatcher, "attachment_v2_transformed_er", "related_observation"),
        (EREventUpdateDispatcher, "event_update_v2_transformed_er", "external_id"),
        (ERObservationDispatcher, "observations_v2_transformed_er", None),
    ]
)
@pytest.mark.parametrize(
    "mock_erclient_class_with_error,er_error",
    [
        ("missing_event_type", er_errors.ERClientBadRequest),
        ("bad_credentials", er_errors.ERClientBadCredentials),
        ("missing_permissions", er_errors.ERClientPermissionDenied),
        ("service_internal_error", er_errors.ERClientInternalError),
        ("service_unreachable_502", er_errors.ERClientServiceUnreachable),
        ("service_unreachable_503", er_errors.ERClientServiceUnreachable),
    ],
    indirect=["mock_erclient_class_with_error"])
@pytest.mark.asyncio
async def test_dispatcher_raises_exception_on_er_api_error(
    dispatcher_class,
    mock_data,
    dispatcher_extra,
    mocker,
    request,
    mock_cache_empty,
    mock_gundi_client,
    mock_erclient_class_with_error,
    mock_get_cloud_storage,
    er_error,
    mock_pubsub_client_with_observation_delivery_failure,
    mock_gundi_client_v2_class,
    destination_integration_v2,
):
    mock_data = request.getfixturevalue(mock_data)
    dispatcher_kwargs = {}
    if dispatcher_extra == "related_observation":
        related_observation = request.getfixturevalue("dispatched_event")
        dispatcher_kwargs["related_observation"] = related_observation
    elif dispatcher_extra == "external_id":
        dispatcher_kwargs["external_id"] = "35cb4b09-18b6-4cf4-9da6-36dd69c6e123"
    await _test_dispatcher_on_errors(
        dispatcher_class=dispatcher_class,
        observation=mock_data.payload,
        mocker=mocker,
        mock_cache_empty=mock_cache_empty,
        mock_gundi_client_v2_class=mock_gundi_client_v2_class,
        mock_erclient_class_with_error=mock_erclient_class_with_error,
        mock_get_cloud_storage=mock_get_cloud_storage,
        er_error=er_error,
        destination_integration_v2=destination_integration_v2,
        **dispatcher_kwargs
    )


@pytest.mark.parametrize(
    "mock_data",
    [
        "event_v2_transformed_er",
        "attachment_v2_transformed_er",
        "event_update_v2_transformed_er",
        "observations_v2_transformed_er",
    ]
)
@pytest.mark.parametrize(
    "mock_erclient_class_with_error,er_error",
    [
        ("missing_event_type", er_errors.ERClientBadRequest),
        ("bad_credentials", er_errors.ERClientBadCredentials),
        ("missing_permissions", er_errors.ERClientPermissionDenied),
        ("service_internal_error", er_errors.ERClientInternalError),
        ("service_unreachable_502", er_errors.ERClientServiceUnreachable),
        ("service_unreachable_503", er_errors.ERClientServiceUnreachable),
    ],
    indirect=["mock_erclient_class_with_error"])
@pytest.mark.asyncio
async def test_dispatch_transformed_observation_v2_publishes_event_on_errors(
        mock_data,
        request,
        mocker,
        mock_cache_empty,
        mock_gundi_client,
        mock_erclient_class_with_error,
        mock_get_cloud_storage,
        er_error,
        mock_publish_event,
        mock_gundi_client_v2_class,
        event_v2_attributes,
        destination_integration_v2,
):
    mock_data = request.getfixturevalue(mock_data)

    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mock_erclient_class_with_error)
    mocker.patch("core.event_handlers.publish_event", mock_publish_event)
    mocker.patch("core.dispatchers.get_cloud_storage", mock_get_cloud_storage)

    with pytest.raises(DispatcherException):
        await dispatch_transformed_observation_v2(
            observation=mock_data,
            attributes=event_v2_attributes
        )

    # Check that the right event was published to the right pubsub topic to inform other services about the error
    assert mock_publish_event.called
    assert mock_publish_event.call_count == 1
    call = mock_publish_event.mock_calls[0]
    assert call.kwargs["topic_name"] == settings.DISPATCHER_EVENTS_TOPIC
    published_event = call.kwargs["event"]
    assert isinstance(published_event, dispatcher_events.ObservationDeliveryFailed)
    assert published_event.event_type == "ObservationDeliveryFailed"
    assert published_event.schema_version == "v2"
    payload = published_event.payload
    assert payload.error_traceback
    assert payload.error
    assert payload.server_response_status
    assert payload.server_response_body


def _make_erclient_mock_for_auth_retry(mocker, post_method_name, side_effect):
    erclient_mock = mocker.MagicMock()
    setattr(erclient_mock, post_method_name, mocker.AsyncMock(side_effect=side_effect))
    erclient_mock.__aenter__ = mocker.AsyncMock(return_value=erclient_mock)
    erclient_mock.__aexit__ = mocker.AsyncMock(return_value=None)
    erclient_mock.close = mocker.AsyncMock(return_value=None)
    erclient_mock.token_url = "https://fake-site.pamdas.org/oauth2/token"
    erclient_mock.username = "fake-username"
    return erclient_mock


@pytest.mark.asyncio
async def test_v2_dispatcher_retries_send_once_on_bad_credentials(
    mocker,
    mock_cache_empty,
    mock_er_bad_credentials_error,
    post_report_response,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker, "post_report", [mock_er_bad_credentials_error, post_report_response]
    )
    mocked_erclient_class = mocker.MagicMock(return_value=erclient_mock)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mocked_erclient_class)
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    result = await dispatcher.send(event_v2_transformed_er.payload)

    assert result == post_report_response
    assert erclient_mock.post_report.await_count == 2
    mock_cache_empty.delete.assert_called_once()  # cached token invalidated
    assert mocked_erclient_class.call_count == 2  # client rebuilt for the retry


@pytest.mark.asyncio
async def test_v2_dispatcher_raises_on_second_bad_credentials(
    mocker,
    mock_cache_empty,
    mock_er_bad_credentials_error,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker,
        "post_report",
        [mock_er_bad_credentials_error, mock_er_bad_credentials_error],
    )
    mocker.patch(
        "core.dispatchers.TokenCachingAsyncERClient",
        mocker.MagicMock(return_value=erclient_mock),
    )
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    with pytest.raises(er_errors.ERClientBadCredentials):
        await dispatcher.send(event_v2_transformed_er.payload)

    assert erclient_mock.post_report.await_count == 2


@pytest.mark.asyncio
async def test_v2_dispatcher_does_not_retry_on_permission_denied(
    mocker,
    mock_cache_empty,
    mock_er_missing_permissions_error,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker, "post_report", [mock_er_missing_permissions_error]
    )
    mocker.patch(
        "core.dispatchers.TokenCachingAsyncERClient",
        mocker.MagicMock(return_value=erclient_mock),
    )
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    with pytest.raises(er_errors.ERClientPermissionDenied):
        await dispatcher.send(event_v2_transformed_er.payload)

    assert erclient_mock.post_report.await_count == 1
    mock_cache_empty.delete.assert_not_called()


@pytest.mark.asyncio
async def test_v2_dispatcher_does_not_retry_static_token_client_on_bad_credentials(
    mocker,
    mock_cache_empty,
    mock_er_bad_credentials_error,
    post_report_response,
    destination_integration_v2,
    event_v2_transformed_er,
):
    mocker.patch("core.er_auth._cache_db", mock_cache_empty)
    erclient_mock = _make_erclient_mock_for_auth_retry(
        mocker, "post_report", [mock_er_bad_credentials_error, post_report_response]
    )
    erclient_mock.username = None
    mocked_erclient_class = mocker.MagicMock(return_value=erclient_mock)
    mocker.patch("core.dispatchers.TokenCachingAsyncERClient", mocked_erclient_class)
    dispatcher = dispatchers.EREventDispatcher(
        integration=destination_integration_v2, provider="fake-provider"
    )

    with pytest.raises(er_errors.ERClientBadCredentials):
        await dispatcher.send(event_v2_transformed_er.payload)

    assert erclient_mock.post_report.await_count == 1
    mock_cache_empty.delete.assert_not_called()
