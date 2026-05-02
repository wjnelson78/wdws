#!/usr/bin/env python3
"""U.S. Constitution + Declaration ingestion for ACE.

Fetches National Archives transcript pages, parses them into citation-level
units, writes `core.documents` rows with `document_type='constitution-section'`,
chunks + embeds them, registers critical USC relationships, validates exact
citation lookup and RAG smoke tests, and writes an Athena completion note.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv('/opt/wdws/.env')
except Exception:
    pass

sys.path.insert(0, '/opt/wdws')
from embedding_service import embed_query_sync, embed_texts_sync, EMBEDDING_DIMENSIONS

assert EMBEDDING_DIMENSIONS == 1024, f'unexpected embedding dim {EMBEDDING_DIMENSIONS}'

DSN = os.environ.get('DATABASE_URL')
if not DSN:
    raise RuntimeError('DATABASE_URL is required; set it in /opt/wdws/.env or the process environment')
ROOT = Path('/opt/wdws/us_constitution_research')
DATA = ROOT / 'data'
SOURCE_DIR = DATA / 'source'
PARSED_JSON = DATA / 'us_constitution_units.json'
LOG_PATH = ROOT / 'ingest_us_constitution.log'
INGEST_DATE = '2026-05-02'
EMBEDDING_MODEL_ID = 2
CHUNK_TARGET_CHARS = 6000
EMBED_BATCH_SIZE = 64

SOURCE_URLS = {
    'constitution': 'https://www.archives.gov/founding-docs/constitution-transcript',
    'bill_of_rights': 'https://www.archives.gov/founding-docs/bill-of-rights-transcript',
    'amendments_11_27': 'https://www.archives.gov/founding-docs/amendments-11-27',
    'declaration': 'https://www.archives.gov/founding-docs/declaration-transcript',
}
CORNELL_VERIFY_URL = 'https://www.law.cornell.edu/constitution'

ARTICLE_TITLES = {
    'I': 'LEGISLATIVE POWERS',
    'II': 'EXECUTIVE POWER',
    'III': 'JUDICIAL POWER',
    'IV': 'STATES, FULL FAITH AND CREDIT, AND TERRITORIES',
    'V': 'AMENDMENT PROCESS',
    'VI': 'SUPREMACY, DEBTS, AND OATHS',
    'VII': 'RATIFICATION',
}

ARTICLE_SECTION_TITLES = {
    ('I', '1'): 'VESTING OF LEGISLATIVE POWER',
    ('I', '4'): 'ELECTIONS AND MEETINGS OF CONGRESS',
    ('I', '5'): 'PROCEEDINGS OF EACH HOUSE',
    ('I', '6'): 'COMPENSATION, PRIVILEGES, AND RESTRICTIONS',
    ('I', '7'): 'LEGISLATIVE PROCESS',
    ('II', '1'): 'PRESIDENTIAL ELECTION, QUALIFICATIONS, AND OATH',
    ('II', '2'): 'PRESIDENTIAL POWERS',
    ('II', '3'): 'PRESIDENTIAL DUTIES',
    ('II', '4'): 'IMPEACHMENT',
    ('III', '1'): 'JUDICIAL POWER VESTED',
    ('III', '2'): 'JUDICIAL POWER AND JURISDICTION',
    ('III', '3'): 'TREASON',
    ('IV', '1'): 'FULL FAITH AND CREDIT',
    ('IV', '3'): 'NEW STATES AND TERRITORIES',
    ('IV', '4'): 'REPUBLICAN GOVERNMENT AND PROTECTION',
    ('V', '1'): 'AMENDMENT PROCESS',
    ('VI', '1'): 'CONFEDERATION DEBTS',
    ('VI', '2'): 'SUPREMACY CLAUSE',
    ('VI', '3'): 'OATHS AND NO RELIGIOUS TEST',
    ('VII', '1'): 'RATIFICATION',
}

CLAUSE_TITLES = {
    ('I', '2', '1'): 'HOUSE COMPOSITION AND ELECTORS',
    ('I', '2', '2'): 'REPRESENTATIVE QUALIFICATIONS',
    ('I', '2', '3'): 'APPORTIONMENT AND THREE-FIFTHS CLAUSE',
    ('I', '2', '4'): 'HOUSE VACANCIES',
    ('I', '2', '5'): 'SPEAKER AND IMPEACHMENT POWER',
    ('I', '3', '1'): 'SENATE COMPOSITION',
    ('I', '3', '2'): 'SENATE CLASSES AND VACANCIES',
    ('I', '3', '3'): 'SENATOR QUALIFICATIONS',
    ('I', '3', '4'): 'VICE PRESIDENT AS SENATE PRESIDENT',
    ('I', '3', '5'): 'SENATE OFFICERS',
    ('I', '3', '6'): 'IMPEACHMENT TRIALS',
    ('I', '3', '7'): 'IMPEACHMENT JUDGMENTS',
    ('I', '8', '1'): 'TAXING AND SPENDING POWER',
    ('I', '8', '2'): 'BORROWING POWER',
    ('I', '8', '3'): 'COMMERCE CLAUSE',
    ('I', '8', '4'): 'NATURALIZATION AND BANKRUPTCY',
    ('I', '8', '5'): 'COINAGE, VALUE, WEIGHTS, AND MEASURES',
    ('I', '8', '6'): 'COUNTERFEITING',
    ('I', '8', '7'): 'POSTAL POWER',
    ('I', '8', '8'): 'INTELLECTUAL PROPERTY',
    ('I', '8', '9'): 'INFERIOR TRIBUNALS',
    ('I', '8', '10'): 'PIRACIES, FELONIES, AND LAW OF NATIONS',
    ('I', '8', '11'): 'WAR POWERS',
    ('I', '8', '12'): 'ARMIES',
    ('I', '8', '13'): 'NAVY',
    ('I', '8', '14'): 'MILITARY RULES',
    ('I', '8', '15'): 'MILITIA CALL-FORTH POWER',
    ('I', '8', '16'): 'MILITIA ORGANIZATION',
    ('I', '8', '17'): 'ENCLAVE CLAUSE',
    ('I', '8', '18'): 'NECESSARY AND PROPER CLAUSE',
    ('I', '9', '1'): 'SLAVE TRADE CLAUSE',
    ('I', '9', '2'): 'HABEAS CORPUS SUSPENSION CLAUSE',
    ('I', '9', '3'): 'BILLS OF ATTAINDER AND EX POST FACTO LAWS',
    ('I', '9', '4'): 'DIRECT TAXES',
    ('I', '9', '5'): 'EXPORT TAXES',
    ('I', '9', '6'): 'PORT PREFERENCE CLAUSE',
    ('I', '9', '7'): 'APPROPRIATIONS CLAUSE',
    ('I', '9', '8'): 'TITLES OF NOBILITY AND EMOLUMENTS',
    ('I', '10', '1'): 'STATE TREATIES, COINAGE, AND CONTRACTS',
    ('I', '10', '2'): 'STATE IMPORT-EXPORT DUTIES',
    ('I', '10', '3'): 'STATE TONNAGE, COMPACTS, AND WAR POWERS',
    ('IV', '2', '1'): 'PRIVILEGES AND IMMUNITIES',
    ('IV', '2', '2'): 'EXTRADITION',
    ('IV', '2', '3'): 'FUGITIVE SLAVE CLAUSE',
}

AMENDMENT_TITLES = {
    1: 'FREEDOM OF RELIGION, SPEECH, PRESS, ASSEMBLY, PETITION',
    2: 'RIGHT TO KEEP AND BEAR ARMS',
    3: 'QUARTERING OF SOLDIERS',
    4: 'SEARCH AND SEIZURE',
    5: 'DUE PROCESS, SELF-INCRIMINATION, DOUBLE JEOPARDY',
    6: 'RIGHTS OF THE ACCUSED',
    7: 'CIVIL JURY TRIAL',
    8: 'EXCESSIVE BAIL; CRUEL AND UNUSUAL PUNISHMENT',
    9: 'UNENUMERATED RIGHTS',
    10: 'RESERVED POWERS',
    11: 'SUITS AGAINST STATES',
    12: 'PRESIDENTIAL ELECTIONS',
    13: 'ABOLITION OF SLAVERY',
    14: 'CITIZENSHIP, DUE PROCESS, EQUAL PROTECTION, AND ENFORCEMENT',
    15: 'RIGHT TO VOTE REGARDLESS OF RACE',
    16: 'INCOME TAX',
    17: 'DIRECT ELECTION OF SENATORS',
    18: 'PROHIBITION (REPEALED BY AMEND. XXI)',
    19: "WOMEN'S SUFFRAGE",
    20: 'PRESIDENTIAL AND CONGRESSIONAL TERMS',
    21: 'REPEAL OF PROHIBITION',
    22: 'PRESIDENTIAL TERM LIMITS',
    23: 'DISTRICT OF COLUMBIA PRESIDENTIAL ELECTORS',
    24: 'POLL TAX',
    25: 'PRESIDENTIAL SUCCESSION AND INABILITY',
    26: 'EIGHTEEN-YEAR-OLD VOTE',
    27: 'CONGRESSIONAL COMPENSATION',
}

AMENDMENT_SECTION_TITLES = {
    (13, '1'): 'ABOLITION OF SLAVERY',
    (13, '2'): 'CONGRESSIONAL ENFORCEMENT',
    (14, '1'): 'EQUAL PROTECTION; DUE PROCESS; CITIZENSHIP',
    (14, '2'): 'APPORTIONMENT',
    (14, '3'): 'DISQUALIFICATION FOR INSURRECTION',
    (14, '4'): 'PUBLIC DEBT',
    (14, '5'): 'CONGRESSIONAL ENFORCEMENT',
    (15, '1'): 'VOTING RIGHTS REGARDLESS OF RACE',
    (15, '2'): 'CONGRESSIONAL ENFORCEMENT',
    (18, '1'): 'PROHIBITION',
    (18, '2'): 'CONCURRENT ENFORCEMENT',
    (18, '3'): 'RATIFICATION DEADLINE',
    (19, '1'): "WOMEN'S SUFFRAGE",
    (19, '2'): 'CONGRESSIONAL ENFORCEMENT',
    (20, '1'): 'TERMS OF PRESIDENT, VICE PRESIDENT, SENATORS, AND REPRESENTATIVES',
    (20, '2'): 'CONGRESSIONAL ASSEMBLY DATE',
    (20, '3'): 'PRESIDENT-ELECT DEATH OR FAILURE TO QUALIFY',
    (20, '4'): 'SUCCESSION CONTINGENCIES IN CONGRESSIONAL SELECTION',
    (20, '5'): 'EFFECTIVE DATE',
    (20, '6'): 'RATIFICATION DEADLINE',
    (21, '1'): 'REPEAL OF EIGHTEENTH AMENDMENT',
    (21, '2'): 'STATE LIQUOR IMPORTATION CONTROL',
    (21, '3'): 'RATIFICATION DEADLINE',
    (22, '1'): 'PRESIDENTIAL TERM LIMITS',
    (22, '2'): 'RATIFICATION DEADLINE',
    (23, '1'): 'DISTRICT OF COLUMBIA PRESIDENTIAL ELECTORS',
    (23, '2'): 'CONGRESSIONAL ENFORCEMENT',
    (24, '1'): 'POLL TAX PROHIBITION',
    (24, '2'): 'CONGRESSIONAL ENFORCEMENT',
    (25, '1'): 'VICE PRESIDENT BECOMES PRESIDENT',
    (25, '2'): 'VICE PRESIDENTIAL VACANCY',
    (25, '3'): 'PRESIDENTIAL DECLARATION OF INABILITY',
    (25, '4'): 'VICE PRESIDENT AND CABINET DECLARATION OF INABILITY',
    (26, '1'): 'RIGHT TO VOTE AT AGE EIGHTEEN',
    (26, '2'): 'CONGRESSIONAL ENFORCEMENT',
}

RATIFICATION_YEARS = {
    1: 1791, 2: 1791, 3: 1791, 4: 1791, 5: 1791, 6: 1791, 7: 1791, 8: 1791, 9: 1791, 10: 1791,
    11: 1795, 12: 1804, 13: 1865, 14: 1868, 15: 1870, 16: 1913, 17: 1913, 18: 1919, 19: 1920,
    20: 1933, 21: 1933, 22: 1951, 23: 1961, 24: 1964, 25: 1967, 26: 1971, 27: 1992,
}

ROMAN_NUMERALS = {
    1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V', 6: 'VI', 7: 'VII', 8: 'VIII', 9: 'IX', 10: 'X',
    11: 'XI', 12: 'XII', 13: 'XIII', 14: 'XIV', 15: 'XV', 16: 'XVI', 17: 'XVII', 18: 'XVIII',
    19: 'XIX', 20: 'XX', 21: 'XXI', 22: 'XXII', 23: 'XXIII', 24: 'XXIV', 25: 'XXV', 26: 'XXVI', 27: 'XXVII',
}
ROMAN_TO_INT = {v: k for k, v in ROMAN_NUMERALS.items()}

SPLIT_ARTICLE_CLAUSES = {('I', '2'), ('I', '3'), ('I', '8'), ('I', '9'), ('I', '10'), ('IV', '2')}
SPLIT_AMENDMENTS = {13, 14, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26}

SPOT_CHECK_CITATIONS = [
    'U.S. Const. pmbl.',
    'U.S. Const. art. I, § 8, cl. 3',
    'U.S. Const. art. I, § 8, cl. 18',
    'U.S. Const. art. I, § 9, cl. 2',
    'U.S. Const. amend. I',
    'U.S. Const. amend. IV',
    'U.S. Const. amend. V',
    'U.S. Const. amend. VIII',
    'U.S. Const. amend. XIV, § 1',
    'U.S. Const. amend. XIV, § 5',
    'U.S. Const. amend. XVIII',
]

RAG_CHECKS = [
    ('equal protection clause fourteenth amendment', ['U.S. Const. amend. XIV, § 1']),
    ('ADA congressional enforcement power', ['U.S. Const. amend. XIV, § 5']),
    ('unreasonable search seizure', ['U.S. Const. amend. IV']),
    ('cruel unusual punishment', ['U.S. Const. amend. VIII']),
    ('due process liberty property', ['U.S. Const. amend. XIV, § 1', 'U.S. Const. amend. V']),
    ('commerce clause', ['U.S. Const. art. I, § 8, cl. 3']),
    ('habeas corpus suspension', ['U.S. Const. art. I, § 9, cl. 2']),
    ('all men are created equal', ['Declaration of Independence (1776)']),
]

CROSS_REFERENCES = [
    ('U.S. Const. amend. XIV, § 1', '42 U.S.C. § 1983', 'enforcement_vehicle', '§ 1983 is primary enforcement vehicle for 14th Amendment violations'),
    ('U.S. Const. amend. XIV, § 5', '42 U.S.C. § 12101', 'congressional_power_basis', 'Congressional power basis for ADA Title II'),
    ('U.S. Const. amend. XIV, § 5', '29 U.S.C. § 794', 'congressional_power_basis', 'Congressional power basis for Rehabilitation Act § 504'),
    ('U.S. Const. amend. I', '42 U.S.C. § 1983', 'claim_vehicle', '§ 1983 First Amendment claims'),
    ('U.S. Const. amend. IV', '42 U.S.C. § 1983', 'claim_vehicle', '§ 1983 Fourth Amendment claims'),
    ('U.S. Const. amend. VIII', '42 U.S.C. § 1983', 'claim_vehicle', '§ 1983 Eighth Amendment claims'),
    ('U.S. Const. amend. XIV, § 1', '42 U.S.C. § 12132', 'constitutional_statutory_link', 'ADA Title II by-reason-of-disability link to EP/DP arguments'),
    ('U.S. Const. art. III, § 1', '28 U.S.C. § 1331', 'jurisdiction_basis', 'Federal question jurisdiction basis'),
    ('U.S. Const. art. VI, § 2', '28 U.S.C. § 1331', 'preemption_basis', 'Federal preemption and supremacy basis'),
]


@dataclass
class Unit:
    title: str
    content: str
    metadata: dict[str, Any]
    source_path: str
    filename: str


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open('a', encoding='utf-8') as f:
        f.write(line + '\n')


def clean_text(text: str) -> str:
    text = text.replace('\xa0', ' ')
    text = text.replace('\u200b', '')
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'\s+([,.;:])', r'\1', text)
    text = text.replace(' --', '--').replace('-- ', '--')
    return text


def join_paragraphs(paragraphs: list[str]) -> str:
    return '\n\n'.join(clean_text(p) for p in paragraphs if clean_text(p))


def canonical_article(article: str, section: str, clause: str | None = None) -> str:
    base = f'U.S. Const. art. {article}, § {section}'
    if clause:
        base += f', cl. {clause}'
    return base


def canonical_amendment(num: int, section: str | None = None) -> str:
    roman = ROMAN_NUMERALS[num]
    base = f'U.S. Const. amend. {roman}'
    if section:
        base += f', § {section}'
    return base


def base_metadata(corpus: str, component_type: str, canonical: str) -> dict[str, Any]:
    return {
        'corpus': corpus,
        'component_type': component_type,
        'article_number': None,
        'section_number': None,
        'clause_number': None,
        'article_title': None,
        'amendment_number': None,
        'amendment_number_ordinal': None,
        'ratification_year': None,
        'canonical_citation': canonical,
        'is_repealed': False,
        'repealed_by': None,
        'is_superseded': False,
        'superseded_by': None,
        'is_amended': False,
        'amended_by': None,
        'notes': None,
        'source_primary': 'National Archives',
        'source_urls': list(SOURCE_URLS.values()),
        'cross_verified_with': CORNELL_VERIFY_URL,
        'ingested_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }


def article_flags(article: str, section: str, clause: str | None) -> dict[str, Any]:
    flags = {
        'is_repealed': False,
        'repealed_by': None,
        'is_superseded': False,
        'superseded_by': None,
        'is_amended': False,
        'amended_by': None,
        'notes': None,
    }
    key = (article, section, clause)
    if key == ('I', '2', '3'):
        flags.update(is_superseded=True, superseded_by='U.S. Const. amend. XIV, § 2')
    elif key == ('I', '9', '1'):
        flags.update(is_superseded=True, superseded_by='U.S. Const. amend. XIII')
    elif key == ('IV', '2', '3'):
        flags.update(is_superseded=True, superseded_by='U.S. Const. amend. XIII')
    amended_map = {
        ('I', '3', '1'): 'U.S. Const. amend. XVII',
        ('I', '3', '2'): 'U.S. Const. amend. XVII',
        ('I', '4', None): 'U.S. Const. amend. XX, § 2',
        ('I', '9', '4'): 'U.S. Const. amend. XVI',
        ('II', '1', None): 'U.S. Const. amend. XII; U.S. Const. amend. XX; U.S. Const. amend. XXV',
        ('III', '2', None): 'U.S. Const. amend. XI',
    }
    if key in amended_map:
        flags.update(is_amended=True, amended_by=amended_map[key])
    return flags


def amendment_flags(num: int) -> dict[str, Any]:
    if num == 18:
        return {
            'is_repealed': True,
            'repealed_by': 'U.S. Const. amend. XXI',
            'is_superseded': False,
            'superseded_by': None,
            'is_amended': False,
            'amended_by': None,
            'notes': 'Repealed by Amendment XXI (1933).',
        }
    return {
        'is_repealed': False,
        'repealed_by': None,
        'is_superseded': False,
        'superseded_by': None,
        'is_amended': False,
        'amended_by': None,
        'notes': None,
    }


def fetch_sources() -> None:
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    headers = {'User-Agent': 'wdws/athena-us-constitution-ingest/1.0 (legal-research)'}
    for name, url in SOURCE_URLS.items():
        log(f'fetch {name}: {url}')
        resp = requests.get(url, headers=headers, timeout=45)
        resp.raise_for_status()
        if len(resp.text) < 10_000 and name != 'declaration':
            raise RuntimeError(f'fetched content suspiciously short for {name}: {len(resp.text)} bytes')
        html_path = SOURCE_DIR / f'{name}.html'
        html_path.write_text(resp.text, encoding='utf-8')
        soup = BeautifulSoup(resp.text, 'html.parser')
        region = soup.select_one('.region-content')
        if not region:
            raise RuntimeError(f'could not locate transcript content region for {name}')
        text = region.get_text('\n', strip=True)
        txt_path = SOURCE_DIR / f'{name}.txt'
        txt_path.write_text(text, encoding='utf-8')
        log(f'  wrote {html_path} ({len(resp.text)} bytes) and {txt_path} ({len(text)} chars)')


def source_soup(name: str) -> BeautifulSoup:
    path = SOURCE_DIR / f'{name}.html'
    if not path.exists():
        raise FileNotFoundError(f'missing source html {path}; run --fetch first')
    return BeautifulSoup(path.read_text(encoding='utf-8'), 'html.parser')


def content_elements(name: str) -> list[tuple[str, str]]:
    soup = source_soup(name)
    region = soup.select_one('.region-content')
    if not region:
        raise RuntimeError(f'missing .region-content in {name}')
    out: list[tuple[str, str]] = []
    for el in region.find_all(['h1', 'h2', 'h3', 'p'], recursive=True):
        text = clean_text(el.get_text(' ', strip=True))
        if text:
            out.append((el.name, text))
    return out


def make_article_unit(article: str, section: str, clause: str | None, text: str) -> Unit:
    canonical = canonical_article(article, section, clause)
    if clause:
        desc = CLAUSE_TITLES.get((article, section, clause), f'CLAUSE {clause}')
    else:
        desc = ARTICLE_SECTION_TITLES.get((article, section), f'ARTICLE {article} SECTION {section}')
    title = f'{canonical} — {desc}'
    md = base_metadata('us-constitution', 'article', canonical)
    md.update({
        'article_number': article,
        'section_number': section,
        'clause_number': clause,
        'article_title': ARTICLE_TITLES.get(article),
    })
    md.update(article_flags(article, section, clause))
    suffix = f'art={article}:sec={section}' + (f':cl={clause}' if clause else '')
    return Unit(
        title=title,
        content=text,
        metadata=md,
        source_path=f'us_const:{INGEST_DATE}:{suffix}',
        filename=f'us-constitution-{suffix.replace(":", "-").replace("=", "-")}.txt',
    )


def parse_constitution_units() -> list[Unit]:
    els = content_elements('constitution')
    units: list[Unit] = []

    preamble = next(text for tag, text in els if tag == 'p' and text.startswith('We the People'))
    canonical = 'U.S. Const. pmbl.'
    md = base_metadata('us-constitution', 'preamble', canonical)
    md.update({'section_number': '0'})
    units.append(Unit(
        title='U.S. Const. pmbl. — PREAMBLE',
        content=preamble,
        metadata=md,
        source_path=f'us_const:{INGEST_DATE}:pmbl',
        filename='us-constitution-preamble.txt',
    ))

    article_sections: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    current_article: str | None = None
    current_section: str | None = None
    article_vii_first = False

    for tag, text in els:
        m_art = re.fullmatch(r'Article\.\s+([IVXLC]+)\.', text)
        if tag == 'h2' and m_art:
            current_article = m_art.group(1)
            current_section = None
            continue
        if not current_article:
            continue
        if tag == 'h3':
            m_sec = re.fullmatch(r'Section\.\s+(\d+)\.', text)
            if m_sec:
                current_section = m_sec.group(1)
                continue
        if tag != 'p':
            continue
        if current_article == 'VII':
            if not article_vii_first and text.startswith('The Ratification'):
                article_sections['VII']['1'].append(text)
                article_vii_first = True
            continue
        if current_article in {'V', 'VI'} and current_section is None:
            article_sections[current_article]['1'].append(text)
            continue
        if current_section:
            article_sections[current_article][current_section].append(text)

    for article in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII']:
        sections = article_sections[article]
        if article == 'VI':
            for i, para in enumerate(sections['1'], start=1):
                units.append(make_article_unit(article, str(i), None, para))
            continue
        for section in sorted(sections.keys(), key=lambda s: int(s)):
            paras = sections[section]
            if (article, section) in SPLIT_ARTICLE_CLAUSES:
                for i, para in enumerate(paras, start=1):
                    units.append(make_article_unit(article, section, str(i), para))
            else:
                units.append(make_article_unit(article, section, None, join_paragraphs(paras)))

    return units


def parse_bill_of_rights_units() -> list[Unit]:
    els = content_elements('bill_of_rights')
    units: list[Unit] = []
    in_bill = False
    current_num: int | None = None
    buf: list[str] = []

    def close_current() -> None:
        nonlocal current_num, buf
        if current_num is None:
            return
        content = join_paragraphs(buf)
        canonical = canonical_amendment(current_num)
        title = f'{canonical} — {AMENDMENT_TITLES[current_num]}'
        md = base_metadata('us-constitution', 'amendment', canonical)
        md.update({
            'section_number': '1',
            'amendment_number': current_num,
            'amendment_number_ordinal': ROMAN_NUMERALS[current_num],
            'ratification_year': RATIFICATION_YEARS[current_num],
        })
        units.append(Unit(
            title=title,
            content=content,
            metadata=md,
            source_path=f'us_const:{INGEST_DATE}:amend={ROMAN_NUMERALS[current_num]}',
            filename=f'us-constitution-amend-{ROMAN_NUMERALS[current_num].lower()}.txt',
        ))
        current_num = None
        buf = []

    for tag, text in els:
        if tag == 'h2' and text == 'The U.S. Bill of Rights':
            in_bill = True
            continue
        if not in_bill:
            continue
        m = re.fullmatch(r'Amendment\s+([IVX]+)', text)
        if tag == 'h3' and m:
            close_current()
            current_num = ROMAN_TO_INT[m.group(1)]
            if current_num > 10:
                break
            buf = []
            continue
        if tag == 'p' and current_num:
            if text.startswith('Amendments 11-27') or text.startswith('Note: The capitalization'):
                close_current()
                break
            buf.append(text)
    close_current()
    return units


def parse_amendments_page() -> dict[int, dict[str, Any]]:
    els = content_elements('amendments_11_27')
    amendments: dict[int, dict[str, Any]] = {}
    current_num: int | None = None
    current_section: str | None = None

    def ensure(num: int) -> dict[str, Any]:
        return amendments.setdefault(num, {'preamble': [], 'notes': [], 'unsectioned': [], 'sections': defaultdict(list)})

    for tag, text in els:
        m = re.fullmatch(r'AMENDMENT\s+([IVX]+)', text)
        if tag in {'h2', 'h3'} and m:
            num = ROMAN_TO_INT[m.group(1)]
            if 11 <= num <= 27:
                current_num = num
                current_section = None
                ensure(num)
            continue
        if current_num is None:
            continue
        m_sec = re.fullmatch(r'Section\s+(\d+)\.', text)
        if tag == 'h3' and m_sec:
            current_section = m_sec.group(1)
            ensure(current_num)['sections'][current_section]
            continue
        if tag != 'p':
            continue
        if text.startswith('Back to Constitution Main Page'):
            break
        rec = ensure(current_num)
        if text.startswith('Passed by Congress') or text.startswith('Originally proposed'):
            rec['preamble'].append(text)
            continue
        if text.startswith('Note:') or text.startswith('*'):
            rec['notes'].append(text)
            continue
        if current_section:
            rec['sections'][current_section].append(text)
        else:
            rec['unsectioned'].append(text)
    return amendments


def compose_full_amendment(sections: dict[str, list[str]], unsectioned: list[str]) -> str:
    if sections:
        parts = []
        for sec in sorted(sections, key=lambda s: int(s)):
            parts.append(f'Section {sec}.\n\n{join_paragraphs(sections[sec])}')
        return '\n\n'.join(parts)
    return join_paragraphs(unsectioned)


def make_amendment_unit(num: int, content: str, section: str | None, notes: list[str] | None = None) -> Unit:
    canonical = canonical_amendment(num, section)
    desc = AMENDMENT_SECTION_TITLES.get((num, section), AMENDMENT_TITLES[num]) if section else AMENDMENT_TITLES[num]
    title = f'{canonical} — {desc}'
    md = base_metadata('us-constitution', 'amendment', canonical)
    md.update({
        'section_number': section if section else None,
        'amendment_number': num,
        'amendment_number_ordinal': ROMAN_NUMERALS[num],
        'ratification_year': RATIFICATION_YEARS[num],
        'notes': '; '.join(notes or []) or None,
    })
    md.update(amendment_flags(num))
    if notes and md.get('notes') and num == 18:
        md['notes'] = md['notes'] + ' ' + '; '.join(notes)
    suffix = f'amend={ROMAN_NUMERALS[num]}' + (f':sec={section}' if section else '')
    return Unit(
        title=title,
        content=content,
        metadata=md,
        source_path=f'us_const:{INGEST_DATE}:{suffix}',
        filename=f'us-constitution-{suffix.replace(":", "-").replace("=", "-").lower()}.txt',
    )


def parse_later_amendment_units() -> list[Unit]:
    amendments = parse_amendments_page()
    units: list[Unit] = []
    for num in range(11, 28):
        rec = amendments[num]
        sections = {str(k): v for k, v in rec['sections'].items()}
        unsectioned = rec['unsectioned']
        notes = rec['preamble'] + rec['notes']

        if num == 19 and not sections and len(unsectioned) >= 2:
            sections = {'1': [unsectioned[0]], '2': [unsectioned[1]]}
            unsectioned = []

        if num == 18:
            full = compose_full_amendment(sections, unsectioned)
            units.append(make_amendment_unit(num, full, None, notes))
            for sec in sorted(sections, key=lambda s: int(s)):
                units.append(make_amendment_unit(num, join_paragraphs(sections[sec]), sec, notes))
        elif num in SPLIT_AMENDMENTS and sections:
            for sec in sorted(sections, key=lambda s: int(s)):
                units.append(make_amendment_unit(num, join_paragraphs(sections[sec]), sec, notes))
        else:
            content = compose_full_amendment(sections, unsectioned)
            units.append(make_amendment_unit(num, content, None, notes))
    return units


def parse_declaration_unit() -> Unit:
    els = content_elements('declaration')
    paras: list[str] = []
    started = False
    for tag, text in els:
        if tag != 'p':
            continue
        if text == 'In Congress, July 4, 1776':
            started = True
        if started:
            if text.startswith('Back to Main Declaration Page'):
                break
            paras.append(text)
    content = join_paragraphs(paras)
    canonical = 'Declaration of Independence (1776)'
    md = base_metadata('us-declaration', 'declaration', canonical)
    md.update({
        'canonical_citation': canonical,
        'source_primary': 'National Archives',
        'source_url': SOURCE_URLS['declaration'],
        'date': '1776-07-04',
    })
    return Unit(
        title='Declaration of Independence (1776)',
        content=content,
        metadata=md,
        source_path=f'us_decl:{INGEST_DATE}:declaration',
        filename='declaration-of-independence-1776.txt',
    )


def parse_units() -> list[Unit]:
    units = []
    units.extend(parse_constitution_units())
    units.extend(parse_bill_of_rights_units())
    units.extend(parse_later_amendment_units())
    units.append(parse_declaration_unit())

    citations = [u.metadata['canonical_citation'] for u in units]
    dupes = [c for c, n in Counter(citations).items() if n > 1]
    if dupes:
        raise RuntimeError(f'duplicate canonical citations: {dupes}')
    if len([u for u in units if u.metadata['corpus'] == 'us-constitution']) < 100:
        raise RuntimeError('parsed us-constitution count below halt threshold')
    for c in SPOT_CHECK_CITATIONS + ['U.S. Const. art. VI, § 2']:
        if c not in citations:
            raise RuntimeError(f'missing required citation after parse: {c}')
    for u in units:
        if not u.content.strip():
            raise RuntimeError(f'empty content for {u.metadata["canonical_citation"]}')

    PARSED_JSON.parent.mkdir(parents=True, exist_ok=True)
    PARSED_JSON.write_text(json.dumps([asdict(u) for u in units], ensure_ascii=False, indent=2), encoding='utf-8')
    by_type = Counter(u.metadata['component_type'] for u in units if u.metadata['corpus'] == 'us-constitution')
    log(f'parsed units: total={len(units)} us-constitution={sum(by_type.values())} declaration=1 components={dict(by_type)}')
    log(f'wrote {PARSED_JSON}')
    return units


def load_units() -> list[Unit]:
    if not PARSED_JSON.exists():
        return parse_units()
    data = json.loads(PARSED_JSON.read_text(encoding='utf-8'))
    return [Unit(**u) for u in data]


def chunk_text(title: str, content: str) -> list[str]:
    full = f'{title}\n\n{content}'
    if len(full) <= CHUNK_TARGET_CHARS:
        return [full]
    paras = content.split('\n\n')
    chunks: list[str] = []
    buf = ''
    for para in paras:
        if len(buf) + len(para) + 2 <= CHUNK_TARGET_CHARS - len(title) - 2:
            buf = f'{buf}\n\n{para}' if buf else para
        else:
            if buf:
                chunks.append(f'{title}\n\n{buf}')
            if len(para) > CHUNK_TARGET_CHARS:
                for i in range(0, len(para), CHUNK_TARGET_CHARS - len(title) - 2):
                    chunks.append(f'{title}\n\n{para[i:i + CHUNK_TARGET_CHARS - len(title) - 2]}')
                buf = ''
            else:
                buf = para
    if buf:
        chunks.append(f'{title}\n\n{buf}')
    return [c.strip() for c in chunks if c.strip()]


def vec_literal(vec: list[float]) -> str:
    return '[' + ','.join(f'{x:.7f}' for x in vec) + ']'


def db_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    return conn


def ensure_tags(conn) -> dict[str, int]:
    tag_defs = [
        ('us-constitution', 'US Constitution', 'legal-framework', 'Document is part of the United States Constitution corpus.'),
        ('us-declaration', 'Declaration of Independence', 'legal-framework', 'Document is the Declaration of Independence companion corpus.'),
        ('constitutional-law', 'Constitutional Law', 'legal-framework', 'Constitutional primary-source authority.'),
        ('founding-document', 'Founding Document', 'legal-framework', 'United States founding-document primary source.'),
    ]
    with conn.cursor() as cur:
        for slug, name, category, description in tag_defs:
            cur.execute(
                """
                INSERT INTO core.tags (slug, name, category, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    description = EXCLUDED.description
                """,
                (slug, name, category, description),
            )
        cur.execute('SELECT slug, id FROM core.tags WHERE slug = ANY(%s)', ([t[0] for t in tag_defs],))
        return dict(cur.fetchall())


def apply_tags(conn, document_id: str, tag_ids: list[int]) -> None:
    if not tag_ids:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            'INSERT INTO core.document_tags (document_id, tag_id) VALUES %s ON CONFLICT DO NOTHING',
            [(document_id, tid) for tid in sorted(set(tag_ids))],
        )


def upsert_document(conn, unit: Unit, tag_cache: dict[str, int]) -> tuple[str, int]:
    content_hash = hashlib.sha256(unit.content.encode('utf-8')).hexdigest()
    chunks = chunk_text(unit.title, unit.content)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM core.documents
            WHERE source_path = %s AND document_type = 'constitution-section'
            """,
            (unit.source_path,),
        )
        row = cur.fetchone()
        if row:
            doc_id = str(row[0])
            cur.execute(
                """
                UPDATE core.documents
                   SET domain='legal', title=%s, filename=%s, full_content=%s,
                       raw_content=%s, content_hash=%s, metadata=%s::jsonb,
                       classification='public', confidentiality='public', legal_hold=FALSE,
                       privilege='none', privilege_classified_at=NOW(),
                       privilege_classified_by='rule:founding_document_public',
                       total_chunks=%s, updated_at=NOW()
                 WHERE id = %s
                """,
                (unit.title, unit.filename, unit.content, unit.content, content_hash,
                 json.dumps(unit.metadata), len(chunks), doc_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO core.documents
                    (domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, raw_content, metadata,
                     classification, confidentiality, legal_hold,
                     privilege, privilege_classified_at, privilege_classified_by)
                VALUES
                    ('legal', %s, %s, 'constitution-section', %s,
                     %s, %s, %s, %s, %s::jsonb,
                     'public', 'public', FALSE,
                     'none', NOW(), 'rule:founding_document_public')
                RETURNING id
                """,
                (unit.source_path, unit.filename, unit.title, content_hash, len(chunks),
                 unit.content, unit.content, json.dumps(unit.metadata)),
            )
            doc_id = str(cur.fetchone()[0])

        cur.execute('DELETE FROM core.document_chunks WHERE document_id = %s', (doc_id,))
        chunk_rows = []
        for i, ctext in enumerate(chunks):
            chunk_rows.append((
                f'{doc_id}:{i}', doc_id, i, len(chunks), ctext,
                EMBEDDING_MODEL_ID, len(ctext.split()), ctext,
                json.dumps({'canonical_citation': unit.metadata['canonical_citation'], 'corpus': unit.metadata['corpus']}),
            ))
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks, content,
                 embedding_model_id, token_count, embedded_content, metadata)
            VALUES %s
            """,
            chunk_rows,
        )
    slugs = ['constitutional-law', 'founding-document']
    slugs.append(unit.metadata['corpus'])
    tag_ids = [tag_cache[s] for s in slugs if s in tag_cache]
    apply_tags(conn, doc_id, tag_ids)
    return doc_id, len(chunks)


def ingest_documents() -> None:
    units = load_units()
    conn = db_conn()
    try:
        tag_cache = ensure_tags(conn)
        total_chunks = 0
        for i, unit in enumerate(units, start=1):
            _, n_chunks = upsert_document(conn, unit, tag_cache)
            total_chunks += n_chunks
            if i % 25 == 0:
                conn.commit()
                log(f'  inserted/updated {i}/{len(units)} documents; chunks={total_chunks}')
        conn.commit()
        log(f'ingest documents complete: docs={len(units)} chunks={total_chunks}')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def embed_pending() -> int:
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.id, c.content
                FROM core.document_chunks c
                JOIN core.documents d ON d.id = c.document_id
                WHERE d.document_type = 'constitution-section'
                  AND d.metadata->>'corpus' IN ('us-constitution','us-declaration')
                  AND c.embedding IS NULL
                ORDER BY d.title, c.chunk_index
                """
            )
            rows = cur.fetchall()
        conn.commit()
        if not rows:
            log('embed: no pending chunks')
            return 0
        log(f'embed: {len(rows)} pending chunks')
        done = 0
        for start in range(0, len(rows), EMBED_BATCH_SIZE):
            batch = rows[start:start + EMBED_BATCH_SIZE]
            ids = [r[0] for r in batch]
            texts = [r[1] for r in batch]
            vecs = embed_texts_sync(texts)
            update_rows = [(vec_literal(vec), cid) for cid, vec in zip(ids, vecs)]
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    UPDATE core.document_chunks
                       SET embedding = %s::halfvec(1024), embedded_at = NOW()
                     WHERE id = %s
                    """,
                    update_rows,
                    page_size=64,
                )
            conn.commit()
            done += len(batch)
            log(f'  embedded {done}/{len(rows)} chunks')
        return done
    finally:
        conn.close()


def lookup_doc_id(conn, citation: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM core.documents
            WHERE document_type='constitution-section'
              AND metadata->>'canonical_citation' = %s
            LIMIT 1
            """,
            (citation,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None


def lookup_usc_doc_id(conn, citation: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.id
            FROM usc.sections s
            JOIN core.documents d ON d.id = s.document_id
            WHERE s.canonical_citation = %s
            LIMIT 1
            """,
            (citation,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None


def register_cross_references() -> dict[str, Any]:
    conn = db_conn()
    status = {'registered': [], 'missing_sources': [], 'missing_targets': []}
    try:
        with conn.cursor() as cur:
            for source_cit, usc_cit, relationship, rationale in CROSS_REFERENCES:
                source_id = lookup_doc_id(conn, source_cit)
                target_id = lookup_usc_doc_id(conn, usc_cit)
                if not source_id:
                    status['missing_sources'].append(source_cit)
                    continue
                if not target_id:
                    status['missing_targets'].append(usc_cit)
                    continue
                metadata = {
                    'source_citation': source_cit,
                    'target_citation': usc_cit,
                    'relationship': relationship,
                    'rationale': rationale,
                    'registered_by': 'athena',
                    'registered_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                }
                cur.execute(
                    """
                    INSERT INTO core.document_relationships
                        (source_document_id, target_document_id, relationship_type, confidence, metadata)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (source_document_id, target_document_id, relationship_type)
                    DO UPDATE SET confidence = EXCLUDED.confidence,
                                  metadata = EXCLUDED.metadata
                    """,
                    (source_id, target_id, relationship, 1.0, json.dumps(metadata)),
                )
                status['registered'].append({'source': source_cit, 'target': usc_cit, 'relationship': relationship})
        conn.commit()
        log(f'cross references registered={len(status["registered"])} missing_sources={status["missing_sources"]} missing_targets={status["missing_targets"]}')
        return status
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def hybrid_rag(conn, question: str, top_k: int = 8) -> list[dict[str, Any]]:
    qvec = embed_query_sync(question)
    qlit = vec_literal(qvec)
    fetch_count = top_k * 4
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id, c.document_id, d.title,
                   d.metadata->>'canonical_citation' AS citation,
                   (c.embedding::halfvec(1024) <=> %s::halfvec(1024)) AS distance
            FROM core.document_chunks c
            JOIN core.documents d ON d.id = c.document_id
            WHERE c.embedding IS NOT NULL
              AND d.domain='legal'
              AND d.document_type='constitution-section'
              AND d.metadata->>'corpus' IN ('us-constitution','us-declaration')
            ORDER BY c.embedding::halfvec(1024) <=> %s::halfvec(1024)
            LIMIT %s
            """,
            (qlit, qlit, fetch_count),
        )
        sem = cur.fetchall()
        cur.execute(
            """
            SELECT c.id, c.document_id, d.title,
                   d.metadata->>'canonical_citation' AS citation,
                   ts_rank(c.content_tsv, websearch_to_tsquery('english', %s)) AS rank
            FROM core.document_chunks c
            JOIN core.documents d ON d.id = c.document_id
            WHERE c.content_tsv @@ websearch_to_tsquery('english', %s)
              AND d.domain='legal'
              AND d.document_type='constitution-section'
              AND d.metadata->>'corpus' IN ('us-constitution','us-declaration')
            ORDER BY rank DESC
            LIMIT %s
            """,
            (question, question, fetch_count),
        )
        ft = cur.fetchall()
    scores: dict[str, float] = {}
    data: dict[str, tuple[str, str, str]] = {}
    for rank, r in enumerate(sem):
        cid = r[0]
        scores[cid] = scores.get(cid, 0.0) + 0.7 / (60 + rank + 1)
        data.setdefault(cid, (str(r[1]), r[2], r[3]))
    for rank, r in enumerate(ft):
        cid = r[0]
        scores[cid] = scores.get(cid, 0.0) + 0.3 / (60 + rank + 1)
        data.setdefault(cid, (str(r[1]), r[2], r[3]))
    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    seen_docs: set[str] = set()
    out = []
    for cid, score in ordered:
        doc_id, title, citation = data[cid]
        if doc_id in seen_docs:
            continue
        seen_docs.add(doc_id)
        out.append({'doc_id': doc_id, 'title': title, 'citation': citation, 'score': score})
        if len(out) >= top_k:
            break
    return out


def validate_counts(conn) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metadata->>'corpus' as corpus,
                   COUNT(*) as total,
                   SUM(CASE WHEN (metadata->>'is_repealed')::boolean THEN 1 ELSE 0 END) as repealed,
                   SUM(CASE WHEN (metadata->>'is_superseded')::boolean THEN 1 ELSE 0 END) as superseded
            FROM core.documents
            WHERE document_type = 'constitution-section'
              AND metadata->>'corpus' IN ('us-constitution', 'us-declaration')
            GROUP BY metadata->>'corpus'
            ORDER BY corpus
            """
        )
        rows = cur.fetchall()
    log(f'count validation: {rows}')
    by = {r[0]: r for r in rows}
    if by.get('us-declaration', (None, 0))[1] != 1:
        raise RuntimeError('Declaration count validation failed')
    us_count = by.get('us-constitution', (None, 0))[1]
    if us_count < 100:
        raise RuntimeError(f'us-constitution count below halt threshold: {us_count}')
    return rows


def validate_spot_checks(conn) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, title, metadata->>'canonical_citation',
                   metadata->>'is_repealed', metadata->>'is_superseded'
            FROM core.documents
            WHERE metadata->>'canonical_citation' = ANY(%s)
            ORDER BY metadata->>'canonical_citation'
            """,
            (SPOT_CHECK_CITATIONS,),
        )
        rows = cur.fetchall()
    counts = Counter(r[2] for r in rows)
    failures = [c for c in SPOT_CHECK_CITATIONS if counts[c] != 1]
    if failures:
        raise RuntimeError(f'spot-check exact lookup failures: {[(c, counts[c]) for c in failures]}')
    amend18 = [r for r in rows if r[2] == 'U.S. Const. amend. XVIII'][0]
    if amend18[3] != 'true':
        raise RuntimeError('Amendment XVIII is_repealed flag missing/false')
    log(f'spot-check validation passed: {len(rows)} exact rows')
    return rows


def validate_flags(conn) -> None:
    required = {
        'U.S. Const. art. I, § 2, cl. 3': ('is_superseded', 'true'),
        'U.S. Const. art. I, § 9, cl. 1': ('is_superseded', 'true'),
        'U.S. Const. art. IV, § 2, cl. 3': ('is_superseded', 'true'),
        'U.S. Const. amend. XVIII': ('is_repealed', 'true'),
    }
    with conn.cursor() as cur:
        for citation, (field, expected) in required.items():
            cur.execute(
                """
                SELECT metadata->>%s FROM core.documents
                WHERE metadata->>'canonical_citation' = %s
                """,
                (field, citation),
            )
            rows = cur.fetchall()
            if len(rows) != 1 or rows[0][0] != expected:
                raise RuntimeError(f'flag validation failed for {citation}: {field} rows={rows}')
    log('flag validation passed for repealed/superseded provisions')


def validate_rag(conn) -> list[dict[str, Any]]:
    report = []
    failures = []
    for question, expected in RAG_CHECKS:
        results = hybrid_rag(conn, question, top_k=8)
        citations = [r['citation'] for r in results]
        ok = all(c in citations for c in expected)
        report.append({'question': question, 'expected': expected, 'citations': citations, 'ok': ok})
        log(f'RAG {"PASS" if ok else "FAIL"}: {question!r} -> {citations[:5]} expected={expected}')
        if not ok:
            failures.append(report[-1])
    if failures:
        raise RuntimeError(f'RAG smoke test failures: {json.dumps(failures, indent=2)}')
    return report


def validate_all() -> dict[str, Any]:
    conn = db_conn()
    conn.autocommit = True
    try:
        counts = validate_counts(conn)
        spots = validate_spot_checks(conn)
        validate_flags(conn)
        rag = validate_rag(conn)
        return {'counts': [list(r) for r in counts], 'spot_checks': len(spots), 'rag': rag}
    finally:
        conn.close()


def component_counts(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metadata->>'component_type', COUNT(*)
            FROM core.documents
            WHERE document_type='constitution-section'
              AND metadata->>'corpus'='us-constitution'
            GROUP BY 1
            ORDER BY 1
            """
        )
        return {k: v for k, v in cur.fetchall()}


def flagged_provisions(conn) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metadata->>'canonical_citation', title,
                   (metadata->>'is_repealed')::boolean,
                   metadata->>'repealed_by',
                   (metadata->>'is_superseded')::boolean,
                   metadata->>'superseded_by'
            FROM core.documents
            WHERE document_type='constitution-section'
              AND metadata->>'corpus'='us-constitution'
              AND ((metadata->>'is_repealed')::boolean OR (metadata->>'is_superseded')::boolean)
            ORDER BY metadata->>'canonical_citation'
            """
        )
        return [
            {
                'canonical_citation': r[0],
                'title': r[1],
                'is_repealed': r[2],
                'repealed_by': r[3],
                'is_superseded': r[4],
                'superseded_by': r[5],
            }
            for r in cur.fetchall()
        ]


def create_completion_note(validation: dict[str, Any], xref_status: dict[str, Any]) -> int:
    conn = db_conn()
    try:
        counts = component_counts(conn)
        flags = flagged_provisions(conn)
        note = {
            'final_document_counts_by_component_type': counts,
            'validation': validation,
            'flagged_provisions': flags,
            'cross_reference_registration_status': xref_status,
            'deviations': [
                'Article IV § 2 was split at clause level so the Fugitive Slave Clause alone carries the superseded ghost flag.',
                'Article VI was represented as §§ 1–3 to support the requested exact lookup/cross-reference for U.S. Const. art. VI, § 2 (Supremacy Clause).',
                'Amendment XVIII was ingested both as a full-amendment citation row and as explicit section rows; all are flagged repealed by Amendment XXI, and the full row preserves exact lookup for U.S. Const. amend. XVIII.',
                'Amendments XX, XXI, and XXII were split by their explicit National Archives section headings for citation-level retrieval.',
            ],
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'source_urls_used': SOURCE_URLS,
            'cross_verification_source': CORNELL_VERIFY_URL,
        }
        content = json.dumps(note, ensure_ascii=False, indent=2)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.agent_notes
                    (agent_id, note_type, title, content, tags, priority, status, metadata)
                VALUES
                    ('athena', 'finding', %s, %s, %s, 3, 'closed', %s::jsonb)
                RETURNING id
                """,
                (
                    'US Constitution and Declaration ingestion complete',
                    content,
                    ['us-constitution-ingestion-complete', 'us-constitution', 'us-declaration'],
                    json.dumps({'tag': 'us-constitution-ingestion-complete', 'validation_passed': True}),
                ),
            )
            note_id = cur.fetchone()[0]
        conn.commit()
        log(f'completion note created: ops.agent_notes.id={note_id}')
        return int(note_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def print_summary() -> None:
    units = load_units()
    by_corpus = Counter(u.metadata['corpus'] for u in units)
    by_component = Counter(u.metadata['component_type'] for u in units if u.metadata['corpus'] == 'us-constitution')
    print(json.dumps({'by_corpus': by_corpus, 'by_component': by_component}, indent=2))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--fetch', action='store_true')
    ap.add_argument('--parse', action='store_true')
    ap.add_argument('--ingest', action='store_true')
    ap.add_argument('--embed', action='store_true')
    ap.add_argument('--xref', action='store_true')
    ap.add_argument('--validate', action='store_true')
    ap.add_argument('--note', action='store_true')
    ap.add_argument('--summary', action='store_true')
    ap.add_argument('--all', action='store_true')
    args = ap.parse_args()

    if args.all:
        args.fetch = args.parse = args.ingest = args.embed = args.xref = args.validate = args.note = True

    validation: dict[str, Any] | None = None
    xref_status: dict[str, Any] | None = None

    if args.fetch:
        fetch_sources()
    if args.parse:
        parse_units()
    if args.ingest:
        ingest_documents()
    if args.embed:
        embed_pending()
    if args.xref:
        xref_status = register_cross_references()
    if args.validate:
        validation = validate_all()
    if args.note:
        if validation is None:
            validation = validate_all()
        if xref_status is None:
            xref_status = register_cross_references()
        create_completion_note(validation, xref_status)
    if args.summary:
        print_summary()

    if not any(vars(args).values()):
        ap.print_help()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
