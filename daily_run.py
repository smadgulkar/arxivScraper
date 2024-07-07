import scrapy
import re
from scrapy.crawler import CrawlerProcess
import os

# Suppress the ScrapyDeprecationWarning (optional)
os.environ['SCRAPY_SETTINGS_MODULE'] = 'your_project_name.settings'

class ArxivSpider(scrapy.Spider):
    name = 'arxiv_spider'
    start_urls = ['https://arxiv.org/list/q-fin/recent']
    captured_papers = []

    def parse(self, response):
        for paper in response.css('dl#articles > dt'):
            paper_id = paper.css('a::attr(href)').get()
            if paper_id:
                paper_url = f"https://arxiv.org{paper_id}"
                yield scrapy.Request(paper_url, callback=self.parse_paper)

        # Find the "next page" link and follow it
        next_page_links = response.css('ul.pagination a::attr(href)').getall()
        if next_page_links:
            next_page = next_page_links[-1]
            yield response.follow(next_page, self.parse)


    def parse_paper(self, response):
        try:
            title = response.css('h1.title.mathjax ::text').get().strip()
            if not title:
                raise AttributeError("Title not found")

            abstract_elem = response.css('blockquote.abstract.mathjax')
            abstract = "".join(abstract_elem.css('span::text, blockquote::text').getall())
            abstract = abstract.strip() if abstract else ""

            authors = response.css('div.authors a::text').getall()
            authors = [a.strip() for a in authors if a.strip()]

            pdf_link = response.urljoin(response.css(
                'div.extra-services div.full-text a::attr(href)').get())

            if self.is_relevant(title, abstract):
                item = {
                    'title': title,
                    'authors': authors,
                    'abstract': abstract,
                    'pdf_link': pdf_link
                }
                yield item
                self.captured_papers.append(item)

        except AttributeError as e:
            self.logger.warning(
                f"Error extracting data for paper '{title or 'Unknown Title'}': {e}")


    def is_relevant(self, title, abstract):
        keywords_pattern = re.compile(
            r"statistical methods|trading|investing|factor models|low volatility|alpha generation",
            re.IGNORECASE
        )
        text = (title + ' ' + (abstract or ''))
        return keywords_pattern.search(text) is not None


def display_captured_papers(papers):
    print(f"Number of relevant papers captured: {len(papers)}")
    for i, paper in enumerate(papers, 1):
        print(f"\n--- Paper {i} ---")
        print(f"Title: {paper['title']}")
        print(f"Authors: {', '.join(paper['authors'])}")
        print(f"Abstract: {paper['abstract'][:200]}...")
        print(f"PDF Link: {paper['pdf_link']}")


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ArxivSpider)
    process.start()  # This will block until the crawl is finished

    display_captured_papers(ArxivSpider.captured_papers)
