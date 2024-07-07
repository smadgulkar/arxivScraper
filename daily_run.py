import scrapy
import re
import anthropic
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the Anthropic API key from environment variables
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")


class ArxivSpider(scrapy.Spider):
    name = 'arxiv_spider'
    start_urls = ['https://arxiv.org/list/q-fin/recent', 'https://arxiv.org/list/econ.EM/recent']
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
            title_span = response.css('h1.title.mathjax span.descriptor::text').get()
            if title_span == "Title:":
                title = response.css('h1.title.mathjax ::text')[1].get().strip()
            else:
                raise AttributeError("Title not found or formatted unexpectedly")

            abstract_elem = response.css('blockquote.abstract.mathjax')
            abstract = "".join(abstract_elem.css('span::text, blockquote::text').getall())
            abstract = abstract.strip() if abstract else ""

            authors = response.css('div.authors a::text').getall()
            authors = [a.strip() for a in authors if a.strip()]

            pdf_link = response.urljoin(response.css(
                'div.extra-services div.full-text a::attr(href)').get())

            if self.is_relevant(title, abstract):
                # Ask Claude to evaluate the abstract
                evaluation = self.evaluate_abstract(abstract)
                if evaluation["relevance_for_trading"]:
                    item = {
                        'title': title,
                        'authors': authors,
                        'abstract': abstract,
                        'pdf_link': pdf_link,
                        'evaluation': evaluation["reason"]
                    }
                    yield item
                    self.captured_papers.append(item)

        except AttributeError as e:
            self.logger.warning(
                f"Error extracting data for paper '{title or 'Unknown Title'}': {e}")

    def evaluate_abstract(self, abstract):
        c = anthropic.Client(api_key=anthropic_api_key)
        prompt = (f"Please evaluate the following research paper abstract in the context of generating "
                  f"trading ideas for US equity markets:\n\n{abstract}\n\nDoes the abstract discuss "
                  f"concepts or methods that could potentially be used to generate alpha or new trading ideas? "
                  f"If so, provide a brief explanation.")

        response = c.completion(
            prompt=f"\n\nHuman: {prompt}\n\nAssistant:",
            stop_sequences=["\n\nHuman:"],
            model="claude-v1.3",
            max_tokens_to_sample=200,
        )
        reason = response.completion
        relevance_for_trading = "yes" in reason.lower()
        return {"relevance_for_trading": relevance_for_trading, "reason": reason}


def display_captured_papers(papers):
    print(f"Number of relevant papers captured: {len(papers)}")
    for i, paper in enumerate(papers, 1):
        print(f"\n--- Paper {i} ---")
        print(f"Title: {paper['title']}")
        print(f"Authors: {', '.join(paper['authors'])}")
        print(f"Abstract: {paper['abstract'][:200]}...")  # Display first 200 characters of abstract
        print(f"PDF Link: {paper['pdf_link']}")
        print(f"Evaluation: {paper['evaluation']}")


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ArxivSpider)
    process.start()  # This will block until the crawl is finished

    display_captured_papers(ArxivSpider.captured_papers)
