"""
Unified metadata aggregation for books.

Combines Google Books and OpenLibrary data for a given ISBN, with
date normalization and sensible field merging. Also exposes a title
search passthrough to the enhanced search implementation.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import requests


def _normalize_date(val: Optional[str]) -> Optional[str]:
	"""Normalize a date string to ISO (YYYY-MM-DD) suitable for HTML date inputs.

	Handles common formats:
	- YYYY
	- YYYY-MM (pads to first day of month)
	- YYYY-MM-DD
	- MM/DD/YYYY or M/D/YYYY
	- Month D, YYYY (e.g., October 6, 2015)
	Returns None if input is falsy or unparseable.
	"""
	if not val:
		return None

	s = str(val).strip()
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


def _date_specificity(date_str: Optional[str]) -> int:
	"""Return a rough specificity score for a date string before normalization.

	Scores:
	- 3: full date (YYYY-MM-DD, MM/DD/YYYY, Month D, YYYY, etc.)
	- 2: year-month (YYYY-MM or similar)
	- 1: year only (YYYY)
	- 0: unknown/empty

	Heuristic-based; safe for precedence decisions only.
	"""
	if not date_str:
		return 0

	s = str(date_str).strip()
	if not s:
		return 0

	# Full date patterns
	if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
		return 3
	if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
		return 3
	if re.fullmatch(r"[A-Za-z]+\s+\d{1,2},\s*\d{4}", s):
		return 3

	# Year-month
	if re.fullmatch(r"\d{4}-\d{1,2}", s) or re.fullmatch(r"\d{4}/\d{1,2}", s):
		return 2

	# Year-only
	if re.fullmatch(r"\d{4}", s):
		return 1

	# Fallback: try to infer
	m = re.fullmatch(r"(\d{4})(?:[-/](\d{1,2})(?:[-/](\d{1,2}))?)?", s)
	if m:
		return 1 + (1 if m.group(2) else 0) + (1 if m.group(3) else 0)

	return 0


def _fetch_google_by_isbn(isbn: str) -> Dict[str, Any]:
	"""Fetch Google Books metadata for an ISBN.

	Picks the best item by:
	1) Exact ISBN match (10 or 13) in industryIdentifiers
	2) Highest date specificity of volumeInfo.publishedDate
	Fallback to the first item if list is empty.
	"""
	url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
	try:
		resp = requests.get(url, timeout=15)
		resp.raise_for_status()
		data = resp.json()
		items = data.get('items') or []
		if not items:
			return {}

		# Normalize target ISBN (strip non-digits/X)
		import re as _re
		target = _re.sub(r"[^0-9Xx]", "", str(isbn))
		def _extract_isbns(it):
			vi = it.get('volumeInfo', {})
			ids = vi.get('industryIdentifiers', []) or []
			i10 = None
			i13 = None
			for ident in ids:
				t = ident.get('type')
				val = _re.sub(r"[^0-9Xx]", "", str(ident.get('identifier') or ''))
				if t == 'ISBN_10' and val:
					i10 = val
				elif t == 'ISBN_13' and val:
					i13 = val
			return i10, i13

		def _score(it):
			vi = it.get('volumeInfo', {})
			i10, i13 = _extract_isbns(it)
			raw_date = vi.get('publishedDate')
			spec = _date_specificity(raw_date)
			match = 1 if (target and (target == (i13 or '') or target == (i10 or ''))) else 0
			return (match, spec)

		# Choose item with best (match, specificity)
		item = max(items, key=_score)
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

		raw_date = vi.get('publishedDate')
		published_date = _normalize_date(raw_date)
		specificity = _date_specificity(raw_date)

		# Prepare to enhance with full volume fetch
		description = vi.get('description')
		printed_page = vi.get('printedPageCount')
		page_count_val = vi.get('pageCount')
		# Secondary fetch: prefer longer description, printed page count, and real ISBNs
		try:
			vol_id = item.get('id')
			if vol_id:
				full_resp = requests.get(f"https://www.googleapis.com/books/v1/volumes/{vol_id}?projection=full", timeout=15)
				full_resp.raise_for_status()
				full_data = full_resp.json() or {}
				fvi = (full_data.get('volumeInfo') or {})
				# Prefer longer description if available
				full_desc = fvi.get('description')
				if full_desc and (not description or len(str(full_desc)) > len(str(description))):
					description = full_desc
				# Pull printed/page counts from full
				if fvi.get('printedPageCount'):
					printed_page = fvi.get('printedPageCount')
				if fvi.get('pageCount'):
					page_count_val = fvi.get('pageCount')
				# Pull ISBNs if missing
				if not (isbn10 and isbn13):
					fids = fvi.get('industryIdentifiers', []) or []
					for ident in fids:
						if ident.get('type') == 'ISBN_10' and not isbn10:
							isbn10 = ident.get('identifier')
						elif ident.get('type') == 'ISBN_13' and not isbn13:
							isbn13 = ident.get('identifier')
		except Exception:
			pass
		# Fallback to text snippet if still none
		if not description:
			description = (item.get('searchInfo') or {}).get('textSnippet')


		# Categories handling: initial shallow categories vs. richer full volume categories.
		# We prefer any full categories list that either:
		# 1) Has more entries OR
		# 2) Contains at least one hierarchical delimiter ('/' or '>') not present in the initial list.
		# This guards against cases where the initial search only returns a top-level umbrella (e.g. ['Fiction'])
		# while the full volume reveals hierarchical genre paths (e.g. ['Fiction / Science Fiction / Action & Adventure', ...]).
		base_categories = vi.get('categories') or []
		full_cats = []
		try:
			fvi_obj = locals().get('fvi')
			if fvi_obj:
				full_cats = fvi_obj.get('categories') or []
				def _has_hierarchy(cats):
					return any(isinstance(c, str) and ('/' in c or '>' in c) for c in cats)
				if full_cats and (
					len(full_cats) > len(base_categories)
					or _has_hierarchy(full_cats) and not _has_hierarchy(base_categories)
				):
					base_categories = full_cats
		except Exception:
			pass

		# Peel a 'Series:' category if present from Google categories
		series_value = None
		_cleaned = []
		for c in base_categories:
			if isinstance(c, str):
				s = c.strip()
				if s.lower().startswith('series:') and not series_value:
					series_value = s.split(':', 1)[1].strip() or None
				else:
					_cleaned.append(s)
		base_categories = _cleaned

		# Preserve raw hierarchical category path strings; frontend can expand
		raw_category_paths = list(base_categories)

		return {
			'title': vi.get('title') or '',
			'subtitle': vi.get('subtitle') or None,
			'authors': vi.get('authors') or [],
			'publisher': vi.get('publisher') or None,
			'published_date': published_date,
			'published_date_raw': raw_date,
			'published_date_specificity': specificity,
			# Prefer printedPageCount if present (from full or initial response)
			'page_count': printed_page or page_count_val,
			'language': vi.get('language') or 'en',
			'description': description,
			'categories': base_categories,
			'raw_category_paths': raw_category_paths,
			'average_rating': vi.get('averageRating'),
			'rating_count': vi.get('ratingsCount'),
			'cover_url': cover_url,
			'isbn10': isbn10,
			'isbn13': isbn13,
			'google_books_id': item.get('id'),
			'series': series_value,
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
				# Extract ISBNs if present on edition
				isbn10 = None
				isbn13 = None
				try:
					if isinstance(ed.get('isbn_10'), list) and ed['isbn_10']:
						isbn10 = str(ed['isbn_10'][0])
					if isinstance(ed.get('isbn_13'), list) and ed['isbn_13']:
						isbn13 = str(ed['isbn_13'][0])
				except Exception:
					pass
				# Description may be a string or object with 'value'
				desc = ed.get('description')
				if isinstance(desc, dict):
					desc = desc.get('value')

				# Series (edition JSON sometimes includes 'series': ["Wheel of Time"]) 
				def _extract_series(val):
					if not val:
						return None
					try:
						if isinstance(val, list) and val:
							candidate = str(val[0])
						elif isinstance(val, str):
							candidate = val
						else:
							return None
						candidate = candidate.strip()
						import re as _re
						candidate = _re.sub(r'^\s*Series\s*:\s*', '', candidate, flags=_re.IGNORECASE)
						candidate = _re.sub(r'\s*\([^)]*\)\s*$', '', candidate).strip()
						return candidate or None
					except Exception:
						return None

				series_value = _extract_series(ed.get('series'))

				# Categories not available via this lightweight fallback
				categories = []
				return {
					'title': title,
					'authors': [],  # would require extra calls for names
					'publisher': publishers[0] if publishers else None,
					'published_date': _normalize_date(publish_date),
					'published_date_raw': publish_date,
					'published_date_specificity': _date_specificity(publish_date),
					'page_count': number_of_pages,
					'language': None,
					'description': desc,
					'categories': categories,
					'cover_url': None,
					'openlibrary_id': work_path or ed.get('key'),  # prefer works path
					'series': series_value,
					'isbn10': isbn10,
					'isbn13': isbn13,
				}
			except Exception:
				return {}

		# jscmd=data shape
		authors = [a.get('name') for a in (ol.get('authors') or []) if isinstance(a, dict)]
		publishers = [p.get('name') if isinstance(p, dict) else str(p) for p in (ol.get('publishers') or [])]
		raw_date = ol.get('publish_date')
		published_date = _normalize_date(raw_date)
		number_of_pages = ol.get('number_of_pages')
		cover = ol.get('cover') or {}
		cover_url = cover.get('large') or cover.get('medium') or cover.get('small')
		identifiers = ol.get('identifiers') or {}
		# Description normalization (string or dict)
		desc = ol.get('description')
		if isinstance(desc, dict):
			desc = desc.get('value')
		if not desc:
			# Sometimes only 'notes' exists
			notes = ol.get('notes')
			if isinstance(notes, dict):
				desc = notes.get('value')
			elif isinstance(notes, str):
				desc = notes
		# Extract ISBNs from identifiers if available
		isbn10 = None
		isbn13 = None
		try:
			if isinstance(identifiers.get('isbn_10'), list) and identifiers['isbn_10']:
				isbn10 = str(identifiers['isbn_10'][0])
			if isinstance(identifiers.get('isbn_13'), list) and identifiers['isbn_13']:
				isbn13 = str(identifiers['isbn_13'][0])
		except Exception:
			pass
		# Try to get a usable OpenLibrary link path
		ol_key = ol.get('key')  # often '/books/OL...M'
		if not ol_key:
			openlibrary = identifiers.get('openlibrary') or []
			if openlibrary:
				# edition id like 'OL12345M' -> build books path
				ol_key = f"/books/{openlibrary[0]}"

		# Normalize categories (subjects) to names
		def _norm_subjects(subjs):
			out = []
			for s in (subjs or []):
				name = s.get('name') if isinstance(s, dict) else (str(s) if s is not None else None)
				if name and name not in out:
					out.append(name)
			return out

		# Series may appear explicitly in OL 'series' field (string or list)
		def _extract_series(val):
			if not val:
				return None
			try:
				if isinstance(val, list) and val:
					candidate = str(val[0])
				elif isinstance(val, str):
					candidate = val
				else:
					return None
				candidate = candidate.strip()
				# Strip a leading 'Series:' if present and any trailing parenthetical like '(1)'
				import re as _re
				candidate = _re.sub(r'^\s*Series\s*:\s*', '', candidate, flags=_re.IGNORECASE)
				candidate = _re.sub(r'\s*\([^)]*\)\s*$', '', candidate).strip()
				return candidate or None
			except Exception:
				return None

		series_value = _extract_series(ol.get('series'))

		base_categories = _norm_subjects(ol.get('subjects'))

		return {
			'title': ol.get('title'),
			'subtitle': ol.get('subtitle'),
			'authors': authors,
			'publisher': publishers[0] if publishers else None,
			'published_date': published_date,
			'published_date_raw': raw_date,
			'published_date_specificity': _date_specificity(raw_date),
			'page_count': number_of_pages,
			'language': None,
			'description': desc,
			'categories': base_categories,
			'cover_url': cover_url,
			'openlibrary_id': ol_key,
			'series': series_value,
			'isbn10': isbn10,
			'isbn13': isbn13,
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

	# Simple text fields: choose the more complete (longer) text when both present; prefer Google on exact ties
	for key in ['title', 'subtitle', 'publisher']:
		g_val = google.get(key)
		o_val = openlib.get(key)
		if g_val and o_val:
			merged[key] = _choose_longer_text(g_val, o_val)
		else:
			merged[key] = g_val if g_val is not None else o_val

	# Language and ratings: prefer Google, fallback to OpenLibrary
	for key in ['language', 'average_rating', 'rating_count']:
		merged[key] = google.get(key) if google.get(key) is not None else openlib.get(key)

	# Dates: prefer higher specificity, and prefer Google on ties
	g_date = google.get('published_date')
	o_date = openlib.get('published_date')
	g_spec = google.get('published_date_specificity', 0)
	o_spec = openlib.get('published_date_specificity', 0)
	if g_date and o_date:
		if g_spec > o_spec:
			merged['published_date'] = g_date
		elif o_spec > g_spec:
			merged['published_date'] = o_date
		else:
			# Tie: prefer Google
			merged['published_date'] = g_date
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
	seen_authors = set()
	for src in [google.get('authors') or [], openlib.get('authors') or []]:
		for name in src:
			if isinstance(name, str):
				key = name.strip().casefold()
				if key and key not in seen_authors:
					authors.append(name.strip())
					seen_authors.add(key)
	merged['authors'] = authors

	# Extract 'Series:' indicators from categories before merging
	def _peel_series(src: Dict[str, Any]) -> Tuple[Optional[str], List[str]]:
		series_name = None
		cats: List[str] = []
		for c in src.get('categories') or []:
			if isinstance(c, str):
				s = c.strip()
				if s.lower().startswith('series:') and not series_name:
					series_name = s.split(':', 1)[1].strip() or None
				else:
					cats.append(s)
		return series_name, cats

	g_series, g_cats = _peel_series(google)
	o_series, o_cats = _peel_series(openlib)

	# Light normalization before dedupe: trim, collapse spaces, strip trailing errant punctuation
	import re as _re
	def _norm_cat(val: str) -> Optional[str]:
		if not isinstance(val, str):
			return None
		s = val.strip()
		if not s:
			return None
		s = _re.sub(r"\s+", " ", s)  # collapse whitespace
		s = _re.sub(r"[\s\.,;:]+$", "", s).strip()  # strip trailing punctuation/spaces
		return s or None

	# Apply normalization to provider lists
	g_cats = [c for c in (_norm_cat(c) for c in g_cats) if c]
	o_cats = [c for c in (_norm_cat(c) for c in o_cats) if c]

	categories: List[str] = []
	seen_cats = set()
	for src in [g_cats, o_cats]:
		for c in src:
			key = c.casefold()
			if key and key not in seen_cats:
				categories.append(c)
				seen_cats.add(key)
	merged['categories'] = categories

	# Series: prefer explicit Google series, else OpenLibrary series; fallback: None
	merged['series'] = g_series or o_series or google.get('series') or openlib.get('series')

	# Cover: use ONLY Google-provided URL (no processing here). OpenLibrary fallback only if Google missing.
	from app.utils.book_utils import select_highest_google_image, upgrade_google_cover_url
	raw_google_cover = google.get('cover_url') or select_highest_google_image(google.get('image_links_all'))
	if raw_google_cover:
		merged['cover_url'] = upgrade_google_cover_url(raw_google_cover)
		merged['cover_source'] = 'Google Books'
	else:
		fallback_ol = openlib.get('cover_url')
		if fallback_ol:
			merged['cover_url'] = fallback_ol
			merged['cover_source'] = 'OpenLibrary'
		else:
			merged['cover_url'] = None

	# IDs & ISBNs
	merged['google_books_id'] = google.get('google_books_id')
	merged['openlibrary_id'] = openlib.get('openlibrary_id')
	# Prefer Google ISBNs, fallback to OpenLibrary when missing
	def _norm_isbn(v: Optional[str]) -> Optional[str]:
		if not v:
			return None
		import re as _re
		return _re.sub(r"[^0-9Xx]", "", str(v)) or None
	merged['isbn10'] = _norm_isbn(google.get('isbn10')) or _norm_isbn(openlib.get('isbn10'))
	merged['isbn13'] = _norm_isbn(google.get('isbn13')) or _norm_isbn(openlib.get('isbn13'))

	return merged


def fetch_unified_by_isbn(isbn: str) -> Dict[str, Any]:
	"""Fetch and merge Google Books and OpenLibrary metadata for an ISBN (parallel IO)."""
	isbn = (isbn or '').strip()
	if not isbn:
		return {}

	google = {}
	openlib = {}
	with ThreadPoolExecutor(max_workers=2) as ex:
		future_map = {
			ex.submit(_fetch_google_by_isbn, isbn): 'google',
			ex.submit(_fetch_openlibrary_by_isbn, isbn): 'openlib'
		}
		for fut in as_completed(future_map):
			kind = future_map[fut]
			try:
				data = fut.result() or {}
			except Exception:
				data = {}
			if kind == 'google':
				google = data
			else:
				openlib = data

	if not google and not openlib:
		return {}

	# Ensure dates are normalized (already normalized in fetchers, but keep for safety)
	if google.get('published_date'):
		google['published_date'] = _normalize_date(google['published_date'])
	if openlib.get('published_date'):
		openlib['published_date'] = _normalize_date(openlib['published_date'])

	merged = _merge_dicts(google, openlib)

	# Ensure raw_category_paths present for frontend hierarchical handling
	if 'raw_category_paths' not in merged:
		cats = merged.get('categories') or []
		merged['raw_category_paths'] = list(cats)

	# Ensure we propagate the queried ISBN when providers omit identifiers
	try:
		import re as _re
		raw = _re.sub(r"[^0-9Xx]", "", isbn)
		if raw:
			if not merged.get('isbn13') and len(raw) == 13:
				merged['isbn13'] = raw
			if not merged.get('isbn10') and len(raw) == 10:
				merged['isbn10'] = raw
	except Exception:
		pass

	return merged


def fetch_unified_by_title(title: str, max_results: int = 10, author: Optional[str] = None) -> List[Dict[str, Any]]:
	"""Passthrough to enhanced title search across Google Books and OpenLibrary, with optional author filter."""
	from app.utils.book_search import search_books_by_title
	return search_books_by_title(title, max_results, author)

