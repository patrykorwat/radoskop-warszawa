#!/usr/bin/env python3
"""
Scraper danych głosowań Rady Miasta Stołecznego Warszawy.

Źródło: um.warszawa.pl/waw/radawarszawy
Portal oparty na Liferay — dane renderowane server-side w HTML.

Krok 1: Pobierz listę sesji (ze strony sesji)
Krok 2: Dla każdej sesji — pobierz listę głosowań
Krok 3: Dla każdego głosowania — pobierz wyniki imienne
Krok 4: Wygeneruj data.json w formacie Radoskop

Użycie:
    pip install requests beautifulsoup4 lxml
    python scrape_warszawa.py [--kadencja 2024-2029] [--output docs/data.json]

UWAGA: Uruchom lokalnie — sandbox Cowork blokuje domeny *.warszawa.pl
"""

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Zainstaluj zależności: pip install requests beautifulsoup4 lxml")
    sys.exit(1)

BASE = "https://um.warszawa.pl"
RADA_BASE = f"{BASE}/waw/radawarszawy"

# Znane kadencje
KADENCJE = {
    "2024-2029": {"label": "Kadencja 2024–2029", "sesje_url": f"{RADA_BASE}/sesje"},
    # Dodaj starsze kadencje w razie potrzeby
}

HEADERS = {
    "User-Agent": "Radoskop/1.0 (https://radoskop.pl; open-source city council monitor)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "pl,en;q=0.5",
}

DELAY = 1.0  # sekunda między requestami — bądźmy uprzejmi


def fetch(url: str) -> BeautifulSoup:
    """Pobierz stronę i zwróć BeautifulSoup."""
    time.sleep(DELAY)
    print(f"  GET {url}")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def discover_session_list_url(kadencja_id: str) -> str:
    """Znajdź URL listy sesji dla danej kadencji."""
    # Najpierw sprawdź stronę sesji — mogą być linki do kadencji
    soup = fetch(f"{RADA_BASE}/sesje")

    # Szukaj linków zawierających kadencję
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "sesje" in href.lower() and kadencja_id[:4] in href:
            if href.startswith("/"):
                return BASE + href
            elif href.startswith("http"):
                return href

    # Fallback — główna strona sesji
    return f"{RADA_BASE}/sesje"


def scrape_session_list(kadencja_id: str) -> list[dict]:
    """Pobierz listę sesji z ich datami i linkami."""
    sessions_url = discover_session_list_url(kadencja_id)
    sessions = []
    page = 1

    while True:
        url = sessions_url if page == 1 else f"{sessions_url}?page={page}"
        soup = fetch(url)

        # Szukaj linków do sesji — różne możliwe wzorce
        found_any = False

        # Wzorzec 1: tabela z sesjami
        for row in soup.select("table tr, .sesja-row, .list-group-item"):
            links = row.find_all("a", href=True)
            for link in links:
                href = link["href"]
                if "/sesja/" in href or "sesje" in href.lower():
                    text = link.get_text(strip=True)
                    session_info = parse_session_link(href, text, row)
                    if session_info:
                        sessions.append(session_info)
                        found_any = True

        # Wzorzec 2: lista linków
        if not found_any:
            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True)
                if re.search(r'sesj[aie].*\b[IVXLC]+\b', text, re.IGNORECASE):
                    session_info = parse_session_link(href, text, link.parent)
                    if session_info:
                        sessions.append(session_info)
                        found_any = True

        # Wzorzec 3: Liferay asset publisher entries
        if not found_any:
            for entry in soup.select(".asset-abstract, .journal-content-article, .entry-content"):
                links = entry.find_all("a", href=True)
                text_full = entry.get_text()
                for link in links:
                    session_info = parse_session_link(link["href"], link.get_text(strip=True), entry)
                    if session_info:
                        sessions.append(session_info)
                        found_any = True

        # Paginacja
        next_link = soup.find("a", string=re.compile(r"(następna|next|›|»)", re.I))
        if next_link and found_any:
            page += 1
        else:
            break

    print(f"  Znaleziono {len(sessions)} sesji")
    return sessions


def parse_session_link(href: str, text: str, context_el) -> dict | None:
    """Spróbuj wyciągnąć info o sesji z linka i kontekstu."""
    # Wyciągnij numer sesji (rzymski)
    roman_match = re.search(r'\b([IVXLC]{1,10})\b', text)
    if not roman_match:
        return None

    number = roman_match.group(1)

    # Wyciągnij datę
    date = None
    context_text = context_el.get_text() if context_el else text
    date_match = re.search(r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})', context_text)
    if date_match:
        d, m, y = date_match.groups()
        date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    else:
        date_match2 = re.search(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', context_text)
        if date_match2:
            y, m, d = date_match2.groups()
            date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

    # Normalizuj URL
    url = href
    if url.startswith("/"):
        url = BASE + url
    elif not url.startswith("http"):
        url = RADA_BASE + "/" + url

    return {
        "number": number,
        "date": date,
        "url": url,
    }


def scrape_session_votes(session: dict) -> list[dict]:
    """Pobierz głosowania z danej sesji."""
    soup = fetch(session["url"])
    votes = []

    # Szukaj linków do głosowań
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/glosowanie/" in href:
            vote_id_match = re.search(r'/glosowanie/(\d+)', href)
            if vote_id_match:
                vote_id = vote_id_match.group(1)
                topic = link.get_text(strip=True)
                url = href if href.startswith("http") else BASE + href
                votes.append({
                    "id": vote_id,
                    "topic": topic,
                    "url": url,
                    "session_date": session["date"],
                    "session_number": session["number"],
                })

    print(f"    Sesja {session['number']} ({session['date']}): {len(votes)} głosowań")
    return votes


def scrape_vote_details(vote: dict) -> dict:
    """Pobierz szczegóły głosowania — wyniki imienne."""
    soup = fetch(vote["url"])

    named_votes = {
        "za": [],
        "przeciw": [],
        "wstrzymal_sie": [],
        "brak_glosu": [],
        "nieobecni": [],
    }

    # Mapowanie nazw kategorii z HTML na klucze
    CATEGORY_MAP = {
        "za": "za",
        "tak": "za",
        "przeciw": "przeciw",
        "nie": "przeciw",
        "wstrzymał": "wstrzymal_sie",
        "wstrzymał się": "wstrzymal_sie",
        "wstrzymała się": "wstrzymal_sie",
        "wstrzymali się": "wstrzymal_sie",
        "nie głosował": "brak_glosu",
        "nie głosowała": "brak_glosu",
        "nie głosowali": "brak_glosu",
        "brak głosu": "brak_glosu",
        "nieobecn": "nieobecni",
        "nieobecny": "nieobecni",
        "nieobecna": "nieobecni",
        "nieobecni": "nieobecni",
    }

    # Strategia 1: tabela z kategoriami
    current_category = None
    for el in soup.find_all(["h2", "h3", "h4", "h5", "strong", "b", "th", "td", "p", "li", "div", "span"]):
        text = el.get_text(strip=True).lower()

        # Sprawdź czy to nagłówek kategorii
        for cat_name, cat_key in CATEGORY_MAP.items():
            if cat_name in text and len(text) < 50:
                current_category = cat_key
                break

        # Jeśli mamy kategorię, szukaj nazwisk
        if current_category and el.name in ["li", "td", "p", "span", "div"]:
            name = el.get_text(strip=True)
            # Sprawdź czy to wygląda jak imię i nazwisko
            if re.match(r'^[A-ZŁŚŻŹĆŃ][a-złśżźćńęąó]+\s+[A-ZŁŚŻŹĆŃ]', name) and len(name) < 80:
                # Oczyść z numerków, kropek itp
                name = re.sub(r'^\d+[\.\)]\s*', '', name).strip()
                if name and name.lower() not in CATEGORY_MAP:
                    named_votes[current_category].append(name)

    # Strategia 2: tabela gdzie każdy wiersz to radny + jego głos
    if sum(len(v) for v in named_votes.values()) == 0:
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                name_cell = cells[0].get_text(strip=True)
                vote_cell = cells[-1].get_text(strip=True).lower()
                if re.match(r'^[A-ZŁŚŻŹĆŃ][a-złśżźćńęąó]+\s+[A-ZŁŚŻŹĆŃ]', name_cell):
                    for cat_name, cat_key in CATEGORY_MAP.items():
                        if cat_name in vote_cell:
                            named_votes[cat_key].append(name_cell)
                            break

    # Policz wyniki
    counts = {k: len(v) for k, v in named_votes.items()}

    # Wyciągnij temat/druk jeśli nie mamy
    topic = vote.get("topic", "")
    if not topic or len(topic) < 5:
        # Szukaj tytułu głosowania na stronie
        title_el = soup.find(["h1", "h2"], string=re.compile(r'glosowanie|uchwał|druk|sprawi', re.I))
        if title_el:
            topic = title_el.get_text(strip=True)

    # Druk nr
    druk = None
    druk_match = re.search(r'druk\s*(?:nr\s*)?(\d+[A-Z]?)', topic, re.I)
    if druk_match:
        druk = druk_match.group(1)

    return {
        "id": f"{vote['session_date']}_{vote['id']}",
        "source_id": vote["id"],
        "session_date": vote["session_date"],
        "session_number": vote["session_number"],
        "source_url": vote["url"],
        "topic": topic,
        "druk": druk,
        "resolution": None,
        "counts": counts,
        "named_votes": named_votes,
    }


def build_councilors(all_votes: list[dict], sessions: list[dict]) -> list[dict]:
    """Zbuduj statystyki radnych na podstawie głosowań."""
    # Zbierz wszystkich radnych
    all_names = set()
    for v in all_votes:
        for cat_names in v["named_votes"].values():
            all_names.update(cat_names)

    # Zbierz kluby (na razie nie mamy — trzeba osobno scrape'ować)
    # TODO: dodać scraping klubów z um.warszawa.pl/waw/radawarszawy/radni-2024-2029

    councilors = {}
    for name in sorted(all_names):
        councilors[name] = {
            "name": name,
            "club": "?",  # Trzeba uzupełnić
            "votes_za": 0,
            "votes_przeciw": 0,
            "votes_wstrzymal": 0,
            "votes_brak": 0,
            "votes_nieobecny": 0,
            "votes_total": 0,
            "sessions_present": set(),
            "sessions_total": 0,
            "rebellions": [],
        }

    # Policz głosy
    for v in all_votes:
        for name in v["named_votes"].get("za", []):
            if name in councilors:
                councilors[name]["votes_za"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
        for name in v["named_votes"].get("przeciw", []):
            if name in councilors:
                councilors[name]["votes_przeciw"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
        for name in v["named_votes"].get("wstrzymal_sie", []):
            if name in councilors:
                councilors[name]["votes_wstrzymal"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
        for name in v["named_votes"].get("brak_glosu", []):
            if name in councilors:
                councilors[name]["votes_brak"] += 1
                councilors[name]["sessions_present"].add(v["session_date"])
        for name in v["named_votes"].get("nieobecni", []):
            if name in councilors:
                councilors[name]["votes_nieobecny"] += 1

    total_sessions = len(sessions)
    total_votes = len(all_votes)

    result = []
    for c in councilors.values():
        present_votes = c["votes_za"] + c["votes_przeciw"] + c["votes_wstrzymal"] + c["votes_brak"]
        c["votes_total"] = total_votes
        c["sessions_total"] = total_sessions

        frekwencja = (len(c["sessions_present"]) / total_sessions * 100) if total_sessions > 0 else 0
        aktywnosc = (present_votes / total_votes * 100) if total_votes > 0 else 0

        result.append({
            "name": c["name"],
            "club": c["club"],
            "frekwencja": round(frekwencja, 1),
            "aktywnosc": round(aktywnosc, 1),
            "zgodnosc_z_klubem": 0,  # TODO: obliczyć po uzupełnieniu klubów
            "votes_za": c["votes_za"],
            "votes_przeciw": c["votes_przeciw"],
            "votes_wstrzymal": c["votes_wstrzymal"],
            "votes_brak": c["votes_brak"],
            "votes_nieobecny": c["votes_nieobecny"],
            "votes_total": total_votes,
            "rebellion_count": 0,
            "rebellions": [],
        })

    return sorted(result, key=lambda x: x["name"])


def build_sessions(sessions_raw: list[dict], all_votes: list[dict]) -> list[dict]:
    """Zbuduj dane sesji z listą obecności."""
    votes_by_session = defaultdict(list)
    for v in all_votes:
        votes_by_session[v["session_date"]].append(v)

    result = []
    for s in sessions_raw:
        date = s["date"]
        session_votes = votes_by_session.get(date, [])

        # Lista obecnych — radni, którzy oddali jakikolwiek głos
        attendees = set()
        for v in session_votes:
            for cat in ["za", "przeciw", "wstrzymal_sie", "brak_glosu"]:
                attendees.update(v["named_votes"].get(cat, []))

        result.append({
            "date": date,
            "number": s["number"],
            "vote_count": len(session_votes),
            "attendee_count": len(attendees),
            "attendees": sorted(attendees),
        })

    return sorted(result, key=lambda x: x["date"])


def scrape_clubs() -> dict[str, str]:
    """Spróbuj pobrać przynależność klubową radnych."""
    clubs = {}
    try:
        soup = fetch(f"{RADA_BASE}/radni-2024-2029")
        # Szukaj radnych z klubami — różne wzorce HTML
        for el in soup.find_all(["div", "li", "tr", "article"]):
            text = el.get_text()
            # Szukaj wzorca: Imię Nazwisko ... Klub/Partia
            name_match = re.search(r'([A-ZŁŚŻŹĆŃ][a-złśżźćńęąó]+\s+[A-ZŁŚŻŹĆŃ][a-złśżźćńęąó\-]+)', text)
            if name_match:
                name = name_match.group(1)
                # Szukaj klubu w tym samym kontekście
                club_patterns = [
                    r'(Koalicja Obywatelska|KO)',
                    r'(Prawo i Sprawiedliwość|PiS)',
                    r'(Lewica)',
                    r'(Trzecia Droga|TD)',
                    r'(Konfederacja)',
                    r'(Nowa Lewica|NL)',
                ]
                for pattern in club_patterns:
                    club_match = re.search(pattern, text)
                    if club_match:
                        clubs[name] = club_match.group(1)
                        break
    except Exception as e:
        print(f"  Nie udało się pobrać klubów: {e}")

    return clubs


def main():
    parser = argparse.ArgumentParser(description="Scraper Rady Miasta Warszawy")
    parser.add_argument("--kadencja", default="2024-2029", help="ID kadencji (default: 2024-2029)")
    parser.add_argument("--output", default="docs/data_warszawa.json", help="Plik wyjściowy")
    parser.add_argument("--delay", type=float, default=1.0, help="Opóźnienie między requestami (s)")
    parser.add_argument("--max-sessions", type=int, default=0, help="Maks. sesji do pobrania (0=wszystkie)")
    parser.add_argument("--dry-run", action="store_true", help="Tylko lista sesji, bez głosowań")
    args = parser.parse_args()

    global DELAY
    DELAY = args.delay

    kadencja_id = args.kadencja
    kadencja_info = KADENCJE.get(kadencja_id, {"label": f"Kadencja {kadencja_id}"})

    print(f"=== Radoskop Scraper: Rada m.st. Warszawy ===")
    print(f"Kadencja: {kadencja_info['label']}")
    print()

    # 1. Lista sesji
    print("[1/4] Pobieranie listy sesji...")
    sessions = scrape_session_list(kadencja_id)
    if not sessions:
        print("BŁĄD: Nie znaleziono sesji. Strona mogła zmienić format.")
        print("Sprawdź ręcznie: https://um.warszawa.pl/waw/radawarszawy/sesje")
        sys.exit(1)

    if args.max_sessions > 0:
        sessions = sessions[:args.max_sessions]
        print(f"  (ograniczono do {args.max_sessions} sesji)")

    if args.dry_run:
        print("\nZnalezione sesje:")
        for s in sessions:
            print(f"  {s['number']:>6} | {s['date'] or '???'} | {s['url']}")
        return

    # 2. Głosowania per sesja
    print(f"\n[2/4] Pobieranie głosowań z {len(sessions)} sesji...")
    all_votes_raw = []
    for session in sessions:
        votes = scrape_session_votes(session)
        all_votes_raw.extend(votes)

    print(f"  Razem: {len(all_votes_raw)} głosowań")

    # 3. Szczegóły głosowań
    print(f"\n[3/4] Pobieranie wyników imiennych ({len(all_votes_raw)} głosowań)...")
    all_votes = []
    for i, v in enumerate(all_votes_raw):
        try:
            details = scrape_vote_details(v)
            all_votes.append(details)
            if (i + 1) % 10 == 0:
                print(f"    ... {i+1}/{len(all_votes_raw)}")
        except Exception as e:
            print(f"    BŁĄD głosowanie {v['id']}: {e}")

    # 4. Buduj output
    print(f"\n[4/4] Budowanie pliku wyjściowego...")

    # Spróbuj pobrać kluby
    clubs = scrape_clubs()
    councilors = build_councilors(all_votes, sessions)
    if clubs:
        for c in councilors:
            if c["name"] in clubs:
                c["club"] = clubs[c["name"]]

    sessions_data = build_sessions(sessions, all_votes)

    output = {
        "generated": datetime.now().isoformat(),
        "default_kadencja": kadencja_id,
        "kadencje": [{
            "id": kadencja_id,
            "label": kadencja_info["label"],
            "sessions": sessions_data,
            "total_sessions": len(sessions_data),
            "total_votes": len(all_votes),
            "total_councilors": len(councilors),
            "councilors": councilors,
            "votes": all_votes,
            "similarity_top": [],  # TODO: oblicz po uzupełnieniu klubów
            "similarity_bottom": [],
        }]
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nGotowe! Zapisano do {out_path}")
    print(f"  Sesji: {len(sessions_data)}")
    print(f"  Głosowań: {len(all_votes)}")
    print(f"  Radnych: {len(councilors)}")

    # Podsumowanie jakości
    votes_with_names = sum(1 for v in all_votes if sum(len(nv) for nv in v["named_votes"].values()) > 0)
    print(f"  Głosowań z wynikami imiennymi: {votes_with_names}/{len(all_votes)}")

    if clubs:
        with_club = sum(1 for c in councilors if c["club"] != "?")
        print(f"  Radnych z rozpoznanym klubem: {with_club}/{len(councilors)}")
    else:
        print("  UWAGA: Nie udało się pobrać klubów — trzeba uzupełnić ręcznie")


if __name__ == "__main__":
    main()
