import asyncio
from dataclasses import dataclass, field
from typing import Dict, List

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

BROWSER_CONFIG = BrowserConfig(
    headless=True,
    text_mode=True,
    extra_args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119.0.0.0 Safari/537.36"
)


@dataclass
class CrawlOutput:
    success: Dict[str, str] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class Crawler:
    def __init__(
        self, rate_limit: int = 5, max_retries: int = 3, page_timeout: int = 60000
    ):
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.page_timeout = page_timeout
        self.logger = get_logger()
        self.logger.info(
            f"Crawler initialized | rate_limit={rate_limit}, max_retries={max_retries}, page_timeout={page_timeout}ms"
        )

    async def crawl(self, urls: List[str]) -> CrawlOutput:
        semaphore = asyncio.Semaphore(self.rate_limit)
        result = CrawlOutput()

        async with AsyncWebCrawler(config=BROWSER_CONFIG) as crawler:
            tasks = [
                self._crawl_single(crawler, url, semaphore, result) for url in urls
            ]
            await asyncio.gather(*tasks)

        self.logger.info(
            f"Crawl complete | success={len(result.success)}, errors={len(result.errors)}"
        )
        return result

    async def _crawl_single(
        self,
        crawler: AsyncWebCrawler,
        url: str,
        semaphore: asyncio.Semaphore,
        result: CrawlOutput,
    ):
        async with semaphore:
            try:
                md = await self._crawl_with_retry(crawler, url)
                result.success[url] = md
            except Exception as e:
                self.logger.error(
                    f"Failed to crawl {url} after {self.max_retries} attempts: {e}"
                )
                result.errors[url] = str(e)

    async def _crawl_with_retry(self, crawler: AsyncWebCrawler, url: str) -> str:
        async for attempt in AsyncRetrying(
            wait=wait_exponential(multiplier=1, min=2, max=16),
            stop=stop_after_attempt(self.max_retries),
            reraise=True,
        ):
            with attempt:
                config = CrawlerRunConfig(
                    page_timeout=self.page_timeout,
                    wait_until="networkidle",
                    user_agent=DEFAULT_USER_AGENT,
                )
                response = await crawler.arun(url=url, config=config)
                if not response.success:
                    raise RuntimeError(f"Crawl failed: {response.error_message}")
                return response.markdown
