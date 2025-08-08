"""
Unified metadata aggregation for books.

Combines Google Books and OpenLibrary data for a given ISBN, with
date normalization and sensible field merging. Also exposes a title
search passthrough to the enhanced search implementation.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import re
import requests


def _normalize_date(date_str: Optional[str]) -> Optional[str]:
	"""Normalize a date string to ISO (YYYY-MM-DD) suitable for HTML date inputs.

	Handles common formats:
	- YYYY
	- YYYY-MM (pads to first day of month)
	- YYYY-MM-DD
	- MM/DD/YYYY or M/D/YYYY
	- Month D, YYYY (e.g., October 6, 2015)
	Returns None if input is falsy or unparseable.
	"""
	if not date_str:
		return None

	s = str(date_str).strip()
	if not s:
		return None

	# Already ISO YYYY-MM-DD
	if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
		return s

	# YYYY or YYYY-MM
	m = re.fullmatch(r"(\d{4})(?:-(\d{1,2}))?", s)
	if m:
		year = int(m.group(1))
		month = int(m.group(2)) if m.group(2) else 1
		return f"{year:04d}-{month:02d}-01"

	# MM/DD/YYYY or M/D/YYYY
	m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
	if m:
		month = int(m.group(1))
		day = int(m.group(2))
		year = int(m.group(3))
		return f"{year:04d}-{month:02d}-{day:02d}"

	# Month D, YYYY
	m = re.fullmatch(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", s)
	if m:
		month_name = m.group(1).lower()
		day = int(m.group(2))
		year = int(m.group(3))
		months = {
			'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
			'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
		}
		if month_name in months:
			return f"{year:04d}-{months[month_name]:02d}-{day:02d}"

	# Fallback: if there's a year, return first day of that year
	m = re.search(r"(\d{4})", s)
	if m:
		return f"{int(m.group(1)):04d}-01-01"

	return None


def _fetch_google_by_isbn(isbn: str) -> Dict[str, Any]:
	"""Fetch Google Books metadata for an ISBN."""
	url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
	try:
		resp = requests.get(url, timeout=15)
		resp.raise_for_status()
		data = resp.json()
		items = data.get('items') or []
		if not items:
			return {}

		item = items[0]
		vi = item.get('volumeInfo', {})
		ids = vi.get('industryIdentifiers', [])
		isbn10 = None
		isbn13 = None
		for ident in ids:
			t = ident.get('type')
			if t == 'ISBN_10':
				isbn10 = ident.get('identifier')
			elif t == 'ISBN_13':
				isbn13 = ident.get('identifier')

		# Cover
		cover_url = None
		image_links = vi.get('imageLinks') or {}
		for size in ['extraLarge', 'large', 'medium', 'small', 'thumbnail']:
			if image_links.get(size):
				cover_url = image_links[size].replace('http://', 'https://')
				break

		published_date = _normalize_date(vi.get('publishedDate'))

		return {
			'title': vi.get('title') or '',
			'subtitle': vi.get('subtitle') or None,
			'authors': vi.get('authors') or [],
			'publisher': vi.get('publisher') or None,
			'published_date': published_date,
			'page_count': vi.get('pageCount'),
			'language': vi.get('language') or 'en',
			'description': vi.get('description') or None,
			'categories': vi.get('categories') or [],
			'average_rating': vi.get('averageRating'),
			'rating_count': vi.get('ratingsCount'),
			'cover_url': cover_url,
			'isbn10': isbn10,
			'isbn13': isbn13,
			'google_books_id': item.get('id'),
		}
	except Exception:
		return {}


def _fetch_openlibrary_by_isbn(isbn: str) -> Dict[str, Any]:
	"""Fetch OpenLibrary metadata for an ISBN using the lightweight data API."""
	bibkey = f"ISBN:{isbn}"
	url = f"https://openlibrary.org/api/books?bibkeys={bibkey}&format=json&jscmd=data"
	try:
		resp = requests.get(url, timeout=15)
		resp.raise_for_status()
		data = resp.json() or {}
		ol = data.get(bibkey) or {}
		if not ol:
			# Fallback to edition endpoint to extract an OpenLibrary work path if possible
			try:
				ed_resp = requests.get(f"https://openlibrary.org/isbn/{isbn}.json", timeout=15)
				ed_resp.raise_for_status()
				ed = ed_resp.json() or {}
				work_path = None
				works = ed.get('works') or []
				if works and isinstance(works, list) and isinstance(works[0], dict):
					work_path = works[0].get('key')  # like '/works/OL12345W'
				title = ed.get('title')
				publishers = ed.get('publishers') or []
				publish_date = ed.get('publish_date')
				number_of_pages = ed.get('number_of_pages')
				return {
					'title': title,
					'authors': [],  # would require extra calls for names
					'publisher': publishers[0] if publishers else None,
					'published_date': _normalize_date(publish_date),
					'page_count': number_of_pages,
					'language': None,
					'description': None,
					'categories': [],
					'cover_url': None,
					'openlibrary_id': work_path or ed.get('key'),  # prefer works path
				}
			except Exception:
				return {}

		# jscmd=data shape
		authors = [a.get('name') for a in (ol.get('authors') or []) if isinstance(a, dict)]
		publishers = [p.get('name') if isinstance(p, dict) else str(p) for p in (ol.get('publishers') or [])]
		published_date = _normalize_date(ol.get('publish_date'))
		number_of_pages = ol.get('number_of_pages')
		cover = ol.get('cover') or {}
		cover_url = cover.get('large') or cover.get('medium') or cover.get('small')
		identifiers = ol.get('identifiers') or {}
		# Try to get a usable OpenLibrary link path
		ol_key = ol.get('key')  # often '/books/OL...M'
		if not ol_key:
			openlibrary = identifiers.get('openlibrary') or []
			if openlibrary:
				# edition id like 'OL12345M' -> build books path
				ol_key = f"/books/{openlibrary[0]}"

		return {
			'title': ol.get('title'),
			'subtitle': ol.get('subtitle'),
			'authors': authors,
			'publisher': publishers[0] if publishers else None,
			'published_date': published_date,
			'page_count': number_of_pages,
			'language': None,
			'description': None,
			'categories': ol.get('subjects') or [],
			'cover_url': cover_url,
			'openlibrary_id': ol_key,
		}
	except Exception:
		return {}


def _choose_longer_text(a: Optional[str], b: Optional[str]) -> Optional[str]:
	if a and b:
		return a if len(a) >= len(b) else b
	return a or b or None


def _merge_dicts(google: Dict[str, Any], openlib: Dict[str, Any]) -> Dict[str, Any]:
	"""Merge two metadata dicts with sensible precedence rules."""
	merged: Dict[str, Any] = {}

	# Simple fields: prefer Google, fallback to OpenLibrary
	for key in ['title', 'subtitle', 'publisher', 'language', 'average_rating', 'rating_count']:
		merged[key] = google.get(key) if google.get(key) is not None else openlib.get(key)

	# Dates: prefer a full ISO date if available, otherwise fallback
	g_date = google.get('published_date')
	o_date = openlib.get('published_date')
	# If both present, choose the one with more specificity (length)
	if g_date and o_date:
		merged['published_date'] = g_date if len(g_date) >= len(o_date) else o_date
	else:
		merged['published_date'] = g_date or o_date

	# Page count: take the max if both present
	g_pages = google.get('page_count')
	o_pages = openlib.get('page_count')
	try:
		merged['page_count'] = max(x for x in [g_pages, o_pages] if x is not None)
	except ValueError:
		merged['page_count'] = g_pages or o_pages

	# Description: take the longer text
	merged['description'] = _choose_longer_text(google.get('description'), openlib.get('description'))

	# Authors & categories: union preserving order (Google first)
	authors: List[str] = []
	for src in [google.get('authors') or [], openlib.get('authors') or []]:
		for name in src:
			if isinstance(name, str) and name.strip() and name not in authors:
				authors.append(name)
	merged['authors'] = authors

	categories: List[str] = []
	for src in [google.get('categories') or [], openlib.get('categories') or []]:
		for c in src:
			if isinstance(c, str) and c.strip() and c not in categories:
				categories.append(c)
	merged['categories'] = categories

	# Cover: prefer Google, fallback to OpenLibrary
	merged['cover_url'] = google.get('cover_url') or openlib.get('cover_url')

	# IDs & ISBNs
	merged['google_books_id'] = google.get('google_books_id')
	merged['openlibrary_id'] = openlib.get('openlibrary_id')
	merged['isbn10'] = google.get('isbn10') or None
	merged['isbn13'] = google.get('isbn13') or None

	return merged


def fetch_unified_by_isbn(isbn: str) -> Dict[str, Any]:
	"""Fetch and merge Google Books and OpenLibrary metadata for an ISBN."""
	isbn = (isbn or '').strip()
	if not isbn:
		return {}

	google = _fetch_google_by_isbn(isbn)
	openlib = _fetch_openlibrary_by_isbn(isbn)

	if not google and not openlib:
		return {}

	# Ensure dates are normalized
	if google.get('published_date'):
		google['published_date'] = _normalize_date(google['published_date'])
	if openlib.get('published_date'):
		openlib['published_date'] = _normalize_date(openlib['published_date'])

	return _merge_dicts(google, openlib)


def fetch_unified_by_title(title: str, max_results: int = 10) -> List[Dict[str, Any]]:
	"""Passthrough to enhanced title search across Google Books and OpenLibrary."""
	from app.utils.book_search import search_books_by_title
	return search_books_by_title(title, max_results)

