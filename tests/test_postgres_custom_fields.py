from app.infrastructure.postgres_migration import _REFERENCE_ENTITY_IMPORTS
from app.services.postgres_custom_field_service import PostgresCustomFieldService, is_reserved_core_field


def test_field_definitions_keep_current_and_legacy_kuzu_labels_separate():
    assert _REFERENCE_ENTITY_IMPORTS["CustomField"]["table"] == "custom_field_definitions"
    assert _REFERENCE_ENTITY_IMPORTS["CustomFieldDefinition"]["table"] == "legacy_custom_field_definitions"


def test_custom_field_definition_row_preserves_ownership_scope():
    result = PostgresCustomFieldService._definition((
        "f1", "shelf_color", "Shelf Color", "text", "Book color", "u1", False,
        None, None, 3, "legacy_custom_field_definitions",
    ))

    assert result["name"] == "shelf_color"
    assert result["created_by_user_id"] == "u1"
    assert result["is_global"] is False
    assert result["usage_count"] == 3
    assert result["source_table"] == "legacy_custom_field_definitions"


def test_core_book_fields_are_not_exposed_as_custom_fields():
    assert is_reserved_core_field("SUBTITLE")
    assert is_reserved_core_field("google_books_id")
    assert not is_reserved_core_field("edition")
