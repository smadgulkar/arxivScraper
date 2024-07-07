import scrapy
from bs4 import BeautifulSoup
import PyPDF2
import openai
import schedule
import time
from scrapy.crawler import CrawlerProcess


class ArxivSpider(scrapy.Spider):
    name = 'arxiv_spider'
    start_urls = ['https://arxiv.org/list/q-fin/recent']

    def __init__(self, *args, **kwargs):
        super(ArxivSpider, self).__init__(*args, **kwargs)
        self.papers = []

    def parse(self, response):
        for paper in response.css('dl'):
            title = paper.css('dt .list-title::text').get()
            authors = paper.css('dd.meta .list-authors ::text').getall()
            abstract = paper.css('p.abstract::text').get()
            pdf_link = response.urljoin(paper.css('dt a::attr(href)').get())

            if title and self.is_relevant(title, abstract):
                self.papers.append({
                    'title': title.strip(),
                    'authors': [author.strip() for author in authors if author.strip()],
                    'abstract': abstract.strip() if abstract else '',
                    'pdf_link': pdf_link
                })

        next_page = response.css('div.pagination a:contains("next")::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)

    def is_relevant(self, title, abstract):
        keywords = ['statistical methods', 'trading', 'investing', 'factor models', 'low volatility',
                    'alpha generation']
        text = (title + ' ' + (abstract or '')).lower()
        return any(keyword in text for keyword in keywords)


def extract_pdf_text(pdf_path):
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for page in reader.pages:
            text += page.extract_text() + "\n"
    return text


def summarize_paper(text):
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=f"Summarize the following research paper:\n\n{text[:1000]}...",
        max_tokens=150
    )
    return response.choices[0].text.strip()


def generate_weekly_report(summaries):
    prompt = f"Generate a weekly report summarizing the following research papers:\n\n{summaries}"
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=prompt,
        max_tokens=500
    )
    return response.choices[0].text.strip()


def run_spider():
    process = CrawlerProcess()
    process.crawl(ArxivSpider)
    process.start()


def run_weekly_update():
    run_spider()
    # Here you would typically:
    # 1. Download PDFs for each paper in spider.papers
    # 2. Extract text from each PDF
    # 3. Summarize each paper
    # 4. Generate the weekly report
    # 5. Save or send the report
    print("Weekly update completed")


# Schedule the job
schedule.every().monday.do(run_weekly_update)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)