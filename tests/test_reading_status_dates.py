from datetime import datetime, timezone
from unittest.mock import patch

from app.routes.reading_log_routes import _coerce_existing_timestamp
from app.services.kuzu_service_facade import KuzuServiceFacade


class FakePersonalMetadataService:
    def __init__(self, metadata):
        self.metadata = metadata
        self.updated_kwargs = None

    def get_personal_metadata(self, user_id, book_id):
        return dict(self.metadata)

    def update_personal_metadata(self, user_id, book_id, **kwargs):
        self.updated_kwargs = kwargs
        return dict(self.metadata)


def _facade_with_fake_metadata(fake_metadata):
    facade = object.__new__(KuzuServiceFacade)
    facade.get_book_by_uid_sync = lambda book_id, user_id: {'id': book_id}
    return facade, fake_metadata


def test_on_hold_preserves_existing_start_date():
    fake_metadata = FakePersonalMetadataService(
        {
            'reading_status': 'reading',
            'start_date': '2026-04-01T00:00:00+00:00',
        }
    )
    facade, _ = _facade_with_fake_metadata(fake_metadata)

    with patch(
        'app.services.personal_metadata_service.personal_metadata_service',
        fake_metadata,
    ):
        facade.update_book_sync('book-1', 'user-1', reading_status='on_hold')

    assert fake_metadata.updated_kwargs == {
        'custom_updates': {'reading_status': 'on_hold'},
    }


def test_plan_to_read_keeps_existing_start_date_clearing_behavior():
    fake_metadata = FakePersonalMetadataService(
        {
            'reading_status': 'reading',
            'start_date': '2026-04-01T00:00:00+00:00',
        }
    )
    facade, _ = _facade_with_fake_metadata(fake_metadata)

    with patch(
        'app.services.personal_metadata_service.personal_metadata_service',
        fake_metadata,
    ):
        facade.update_book_sync('book-1', 'user-1', reading_status='plan_to_read')

    assert fake_metadata.updated_kwargs == {
        'custom_updates': {
            'reading_status': 'plan_to_read',
            'start_date': None,
        },
    }


def test_existing_reading_log_timestamp_preserves_kuzu_datetime():
    value = datetime(2026, 2, 13, 3, 41, 48)

    assert _coerce_existing_timestamp(value) is value


def test_existing_reading_log_timestamp_parses_iso_string():
    value = '2026-02-13T03:41:48+00:00'

    assert _coerce_existing_timestamp(value) == datetime(
        2026, 2, 13, 3, 41, 48, tzinfo=timezone.utc
    )
