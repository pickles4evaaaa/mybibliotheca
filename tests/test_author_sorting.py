from app.utils.author_sorting import (
    author_first_sort_key_for_book,
    author_last_sort_key_for_book,
)


def _book(author_name: str, normalized: str | None = None, title: str = ""):
    person = {"name": author_name}
    if normalized is not None:
        person["normalized_name"] = normalized
    return {
        "title": title,
        "contributors": [
            {
                "contribution_type": "authored",
                "person": person,
            }
        ],
    }


def test_author_first_sort_uses_person_normalized_name_for_last_first():
    books = [
        _book("King, Stephen", normalized="stephen king", title="B"),
        _book("Austen, Jane", normalized="jane austen", title="A"),
        _book("Tolkien, J. R. R.", normalized="j. r. r. tolkien", title="C"),
    ]

    books.sort(key=author_first_sort_key_for_book)

    assert [b["contributors"][0]["person"]["name"] for b in books] == [
        "Tolkien, J. R. R.",
        "Austen, Jane",
        "King, Stephen",
    ]


def test_author_last_sort_uses_last_name_primary():
    books = [
        _book("King, Stephen", normalized="stephen king", title="B"),
        _book("Austen, Jane", normalized="jane austen", title="A"),
        _book("Tolkien, J. R. R.", normalized="j. r. r. tolkien", title="C"),
    ]

    books.sort(key=author_last_sort_key_for_book)

    assert [b["contributors"][0]["person"]["name"] for b in books] == [
        "Austen, Jane",
        "King, Stephen",
        "Tolkien, J. R. R.",
    ]


def test_author_first_sort_falls_back_to_parsing_when_normalized_missing():
    books = [
        _book("King, Stephen", normalized=None, title="B"),
        _book("Austen, Jane", normalized=None, title="A"),
    ]

    books.sort(key=author_first_sort_key_for_book)

    assert [b["contributors"][0]["person"]["name"] for b in books] == [
        "Austen, Jane",
        "King, Stephen",
    ]
