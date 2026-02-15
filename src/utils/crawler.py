import asyncio
from dataclasses import dataclass, field
from typing import Dict, List

import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from markdownify import markdownify as md
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

BROWSER_CONFIG = BrowserConfig(
    headless=True,
    text_mode=True,
    enable_stealth=True,
    extra_args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119.0.0.0 Safari/537.36"
)


@dataclass
class CrawlError:
    browser_error: str
    fallback_error: str | None = None


@dataclass
class CrawlOutput:
    success: Dict[str, str] = field(default_factory=dict)
    errors: Dict[str, CrawlError] = field(default_factory=dict)


class Crawler:
    def __init__(
        self,
        rate_limit: int = 5,
        max_retries: int = 3,
        page_timeout: int = 60000,
        verbose: bool = False,
    ):
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.page_timeout = page_timeout
        self.verbose = verbose
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
                markdown = await self._crawl_with_retry(crawler, url)
                result.success[url] = markdown
                return
            except Exception as e:
                browser_error = str(e)
                if self.verbose:
                    self.logger.warning(
                        f"Browser crawl failed for {url} after {self.max_retries} attempts: {browser_error}. Trying httpx fallback..."
                    )
                else:
                    self.logger.warning(
                        f"Browser failed for {url}, trying fallback..."
                    )

            try:
                markdown = await self._fallback_crawl(url)
                self.logger.info(f"Fallback succeeded for {url}")
                result.success[url] = markdown
            except Exception as e:
                fallback_error = str(e)
                if self.verbose:
                    self.logger.error(
                        f"Failed to crawl {url} | browser: {browser_error} | fallback: {fallback_error}"
                    )
                else:
                    self.logger.error(f"Failed to crawl {url}")
                result.errors[url] = CrawlError(
                    browser_error=browser_error,
                    fallback_error=fallback_error,
                )

    async def _crawl_with_retry(self, crawler: AsyncWebCrawler, url: str) -> str:
        async for attempt in AsyncRetrying(
            wait=wait_exponential(multiplier=1, min=2, max=16),
            stop=stop_after_attempt(self.max_retries),
            reraise=True,
        ):
            with attempt:
                config = CrawlerRunConfig(
                    page_timeout=self.page_timeout,
                    wait_until="domcontentloaded",
                    user_agent=DEFAULT_USER_AGENT,
                    magic=True,
                    simulate_user=True,
                    override_navigator=True,
                    verbose=self.verbose,
                )
                response = await crawler.arun(url=url, config=config)
                if not response.success:
                    raise RuntimeError(f"Crawl failed: {response.error_message}")
                return response.markdown

    async def _fallback_crawl(self, url: str) -> str:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=20.0,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            html = response.text
            markdown = md(html, strip=["img", "script", "style"])
            if not markdown or len(markdown.strip()) < 50:
                raise RuntimeError("Fallback returned insufficient content")
            return markdown
