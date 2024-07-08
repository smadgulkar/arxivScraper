import scrapy
import re
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
import os
import anthropic
import json
import logging

# Load environment variables from .env file
load_dotenv()

# Access the Anthropic API key from environment variables
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")

# Set logging level to WARNING or higher to suppress INFO and DEBUG messages
logging.getLogger('scrapy').setLevel(logging.WARNING)


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
        response = c.messages.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }],
            model="claude-3-sonnet-20240229",
            max_tokens=1024,
        )
        reason = response.content[0]
        relevance_for_trading = "yes" in reason.text.lower()  # Correct the relevance check
        return {"relevance_for_trading": relevance_for_trading, "reason": reason.text}

    def is_relevant(self, title, abstract):
        keywords_pattern = re.compile(
            r"statistical\s+methods|trading|investing|factor\s+models|low\s+volatility|alpha\s+generation|"
            r"time\s+series|stochastic\s+processes|econometrics|technical\s+analysis|portfolio\s+optimization|"
            r"option\s+pricing|machine\s+learning|high-frequency\s+trading|HFT|mean\s+reversion|momentum|"
            r"pairs\s+trading|statistical\s+arbitrage|volatility\s+trading|asset\s+pricing|market\s+efficiency|"
            r"risk\s+management|behavioral\s+finance|algorithmic\s+trading|quantitative\s+investment|financial\s+forecasting|"
            r"predictive\s+modeling|pattern\s+recognition|data\s+mining|"
            r"probabilistic|probability|likelihood|uncertainty|random|distribution|Bayesian|Monte Carlo|Markov chain|stochastic volatility",
            re.IGNORECASE  # Case-insensitive matching
        )
        text = (title + ' ' + (abstract or ''))
        return keywords_pattern.search(text) is not None


def display_captured_papers(papers):
    with open("arxiv_paper_report.txt", "w",encoding="utf-8") as f:  # Save report to a file
        f.write(f"Number of relevant papers captured: {len(papers)}\n\n")
        for i, paper in enumerate(papers, 1):
            f.write(f"\n--- Paper {i} ---\n")
            f.write(f"Title: {paper['title']}\n")
            f.write(f"Authors: {', '.join(paper['authors'])}\n")
            f.write(f"Abstract: {paper['abstract']}\n")  # Show the full abstract
            f.write(f"PDF Link: {paper['pdf_link']}\n")
            f.write(f"Evaluation: {paper['evaluation']}\n")

        # Save as JSON for further analysis
        with open("arxiv_paper_data.json", "w") as json_file:
            json.dump(papers, json_file, indent=4)

    print("Report saved to arxiv_paper_report.txt and arxiv_paper_data.json")


if __name__ == "__main__":
    process = CrawlerProcess(settings={'LOG_ENABLED': True,'LOG_LEVEL': 'WARNING'})
    process.crawl(ArxivSpider)
    process.start()  # This will block until the crawl is finished

    display_captured_papers(ArxivSpider.captured_papers)
