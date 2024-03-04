#! /bin/env python3

import os
import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from ruamel.yaml import YAML
import argparse

log_level = os.getenv('LOG_LEVEL', 'WARNING').upper()
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)
logging.getLogger('chardet.charsetprober').setLevel(logging.WARNING)
logging.getLogger('charset_normalizer').setLevel(logging.WARNING)

BASE_URL = "http://www.sfadb.com"
AWARDS = {
    "Hugo": "/Hugo_Awards",
    "Nebula": "/Nebula_Awards",
    "Locus": "/Locus_Awards",
}

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def get_year_links(session, award_url):
    logger.info(f"get_year_links: award_url={award_url}")
    response_text = await fetch(session, award_url)
    soup = BeautifulSoup(response_text, 'html.parser')
    year_links = soup.find_all('a', href=True, string=lambda x: x and x.isdigit())
    return year_links


def make_filename(year=None, title=None, **kwargs):
    return f'{year}-{title}.yml'.replace(' ', '-').replace('/', '-').replace('&', 'and').replace(';', '').replace(':', '')


async def get_novel_winners(session, year_path, award_name):
    year_url = f'{BASE_URL}/{year_path}'
    response_text = await fetch(session, year_url)
    soup = BeautifulSoup(response_text, 'html.parser')
    logger.debug(f"Loaded BeautifulSoup object for URL: {year_url}")

    books = {}
    category_names = ('Novel', 'Sf Novel', 'Fantasy Novel')
    for name in category_names:
        category = soup.find('div', class_='category', string=name)
        if category:
            winner_list = category.find_next_sibling(['ol', 'ul'])
            if winner_list:
                for winner_li in winner_list.find_all('li'):
                    winner_span = winner_li.find('span', class_='winner')
                    if winner_span:
                        title = winner_span.find_next_sibling('b').get_text(strip=True) if winner_span.find_next_sibling('b') else "Title not found"
                        author = winner_span.find_next_sibling('a').get_text(strip=True) if winner_span.find_next_sibling('a') else "Author not found"
                        year = year_url.split('_')[-1]
                        try:
                            year = int(year)
                        except ValueError:
                            logger.error(f"Invalid year format in URL: {year_url}")
                            year = 0
                        if title in books:
                            books[title]['awards'].append(award_name)
                            logger.debug(f"Found tied winner, updated data: Year: {year}, Title: {title}, Author: {author}, Category: {award_name}")
                        else:
                            books[title] = {'year': year, 'title': title, 'author': author, 'awards': [award_name]}
                            logger.debug(f"Extracted Winner data: Year: {year}, Title: {title}, Author: {author}, Category: {award_name}")
    if not books:
        logger.warning(f"None of {category_names} found in {year_url}")
    return books


def save_to_yaml(books, output_file):
    organized_books = {}
    for book in books.values():
        year = book['year']
        if year not in organized_books:
            organized_books[year] = []
        organized_books[year].append({
            'title': book['title'],
            'author': book['author'],
            'awards': book['awards'],
            'year': year
        })

    simplified_structure = []
    for year, books in organized_books.items():
        year_entry = {year: books}
        simplified_structure.append(year_entry)

    with open(output_file, 'w') as file:
        yaml.dump(simplified_structure, file)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetch and save award-winning novel details.')
    parser.add_argument(
        'output_file',
        metavar='FILE',
        nargs='?',
        default='award-winners.yml',
        help='default="%(default)s"; output file to save the award-winning novel details')
    return parser.parse_args()


async def process_awards(session, output_file):
    books = {}
    for award_name, award_path in AWARDS.items():
        logger.info(f"Processing {award_name} award")
        year_links = await get_year_links(session, BASE_URL + award_path)
        for year_link in year_links:
            year_path = year_link['href']
            logger.info(f"Processing year: {year_path}")
            year_books = await get_novel_winners(session, year_path, award_name)
            for title, book_data in year_books.items():
                if title in books:
                    for award in book_data['awards']:
                        if award not in books[title]['awards']:
                            books[title]['awards'].append(award)
                else:
                    books[title] = book_data

    save_to_yaml(books, output_file)


async def main():
    args = parse_arguments()
    async with aiohttp.ClientSession() as session:
        await process_awards(session, args.output_file)


if __name__ == "__main__":
    asyncio.run(main())
