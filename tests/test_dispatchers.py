import base64
import json
from unittest.mock import ANY, call
import pytest
from gundi_core.events import dispatchers as dispatcher_events
from erclient import er_errors
from core import settings
from core.dispatchers import (
    EREventDispatcher,
    EREventAttachmentDispatcher,
    EREventUpdateDispatcher,
    ERObservationDispatcher,
)
from core.event_handlers import dispatch_transformed_observation_v2

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
async def test_er_event_dispatcher_raises_exception_on_er_api_error(
    mocker,
    mock_cache_empty,
    mock_gundi_client,
    mock_erclient_class_with_error,
    er_error,
    mock_pubsub_client_with_observation_delivery_failure,
    mock_gundi_client_v2_class,
    event_v2_transformed_er,
    destination_integration_v2,
    observation_delivery_failure_pubsub_message
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache_empty)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.AsyncERClient", mock_erclient_class_with_error)
    mocker.patch("core.utils.pubsub", mock_pubsub_client_with_observation_delivery_failure)
    # Check that the dispatcher raises an exception so the message is retried later
    with pytest.raises(er_error) as exc_info:
        dispatcher = EREventDispatcher(
            integration=destination_integration_v2,
            provider="test"
        )
        await dispatcher.send(event=event_v2_transformed_er)
    exception = exc_info.value
    expected_error = mock_erclient_class_with_error.return_value.post_report.side_effect
    assert hasattr(exception, "status_code")
    assert hasattr(exception, "response_body")
    assert exception == expected_error


@pytest.mark.asyncio
async def test_dispatch_transformed_observation_v2(mocker):
    # ToDo: Implement test
    pass

