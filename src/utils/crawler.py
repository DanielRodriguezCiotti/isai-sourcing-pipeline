import asyncio
from dataclasses import dataclass, field
from typing import Dict, List

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger


@dataclass
class CrawlOutput:
    success: Dict[str, str] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class Crawler:
    def __init__(
        self, rate_limit: int = 5, max_retries: int = 3, page_timeout: int = 30000
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

        async with AsyncWebCrawler() as crawler:
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
                config = CrawlerRunConfig(page_timeout=self.page_timeout)
                response = await crawler.arun(url=url, config=config)
                if not response.success:
                    raise RuntimeError(f"Crawl failed: {response.error_message}")
                return response.markdown
