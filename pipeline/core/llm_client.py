"""
Shared LLM client with caching, deterministic output (temperature=0), and retry logic.

Single source of truth for all LLM API calls in the pipeline.
All callers use LLMClient.call() instead of building their own HTTP requests.
"""

import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# In-memory response cache keyed on (system_prompt_hash, user_message_hash)
_response_cache: Dict[str, Any] = {}


class LLMClient:
    """
    Deterministic LLM client.

    - temperature=0 for reproducible output
    - Response caching: same input -> same output (no variance on retry)
    - Configurable retry with backoff
    - Single HTTP implementation for all callers
    """

    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        from core.config import get_config
        cfg = get_config()
        self.api_key = api_key or cfg.llm_api_key
        self.base_url = base_url or cfg.llm_base_url
        self.model = model or cfg.llm_model
        self.max_retries = 2
        self.retry_delay = 2  # seconds

    def call(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = 4096,
        use_cache: bool = True,
        cache_key_extra: str = "",
    ) -> Optional[str]:
        """
        Call the LLM API with deterministic settings.

        Args:
            user_message: The user message to send
            system_prompt: System prompt for context
            max_tokens: Maximum response tokens
            use_cache: Whether to use response caching (default True)
            cache_key_extra: Extra string to include in cache key for disambiguation

        Returns:
            Response text string, or None on failure
        """
        if not self.api_key:
            logger.warning("No LLM API key configured")
            return None

        # Check cache
        cache_key = self._cache_key(system_prompt, user_message, cache_key_extra)
        if use_cache and cache_key in _response_cache:
            logger.debug("LLM cache hit")
            try:
                import perf_log as _perf
                _perf.event(
                    "llm_client.call", 0.0,
                    cache_hit=True,
                    cache_key_extra=cache_key_extra,
                    model=self.model,
                )
            except Exception:
                pass
            return _response_cache[cache_key]

        # Build request
        api_url = f"{self.base_url}/v1/messages"
        payload = {
            'model': self.model,
            'max_tokens': max_tokens,
            'temperature': 0,  # Deterministic output - no variance
            'messages': [{'role': 'user', 'content': user_message}],
        }
        if system_prompt:
            payload['system'] = system_prompt

        body = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key,
        }

        # Retry loop
        last_error = None
        call_start = time.monotonic()
        for attempt in range(1, self.max_retries + 1):
            attempt_start = time.monotonic()
            try:
                req = urllib.request.Request(api_url, data=body, headers=headers, method='POST')
                with urllib.request.urlopen(req, timeout=120) as response:
                    data = json.loads(response.read().decode('utf-8'))

                elapsed = time.monotonic() - attempt_start
                text = data.get('content', [{}])[0].get('text', '')
                if text:
                    if use_cache:
                        _response_cache[cache_key] = text
                    total_elapsed = time.monotonic() - call_start
                    logger.info(
                        f"LLM call succeeded: attempt {attempt}, "
                        f"response {elapsed:.1f}s, total {total_elapsed:.1f}s"
                    )
                    try:
                        import perf_log as _perf
                        _perf.event(
                            "llm_client.call", total_elapsed,
                            cache_hit=False,
                            attempts=attempt,
                            cache_key_extra=cache_key_extra,
                            model=self.model,
                            response_chars=len(text),
                        )
                    except Exception:
                        pass
                    return text

                logger.warning(f"LLM returned empty response on attempt {attempt}")

            except urllib.error.HTTPError as e:
                elapsed = time.monotonic() - attempt_start
                error_body = e.read().decode('utf-8')[:500] if e.fp else str(e)
                last_error = f"HTTP {e.code}: {error_body}"

                # Extract rate-limit headers if present
                retry_after = None
                if hasattr(e, 'headers'):
                    retry_after = e.headers.get('retry-after')
                    rl_remaining = e.headers.get('x-ratelimit-remaining')
                    rl_reset = e.headers.get('x-ratelimit-reset')
                    if rl_remaining is not None or rl_reset is not None:
                        logger.warning(
                            f"LLM rate-limit headers: remaining={rl_remaining}, "
                            f"reset={rl_reset}, retry-after={retry_after}"
                        )

                if e.code == 429:
                    logger.warning(
                        f"LLM RATE LIMITED (429) on attempt {attempt}/{self.max_retries} "
                        f"after {elapsed:.1f}s. retry-after={retry_after}. "
                        f"Body: {error_body}"
                    )
                elif e.code == 529:
                    logger.warning(
                        f"LLM OVERLOADED (529) on attempt {attempt}/{self.max_retries} "
                        f"after {elapsed:.1f}s. Body: {error_body}"
                    )
                else:
                    logger.warning(
                        f"LLM API error (attempt {attempt}/{self.max_retries}, "
                        f"{elapsed:.1f}s): {last_error}"
                    )
            except urllib.error.URLError as e:
                elapsed = time.monotonic() - attempt_start
                last_error = f"Connection error: {e.reason}"
                logger.warning(
                    f"LLM API connection error (attempt {attempt}/{self.max_retries}, "
                    f"{elapsed:.1f}s): {last_error}"
                )
            except Exception as e:
                elapsed = time.monotonic() - attempt_start
                last_error = str(e)
                logger.warning(
                    f"LLM call failed (attempt {attempt}/{self.max_retries}, "
                    f"{elapsed:.1f}s): {last_error}"
                )

            if attempt < self.max_retries:
                backoff = self.retry_delay * attempt
                total_so_far = time.monotonic() - call_start
                logger.info(
                    f"LLM retry backoff: sleeping {backoff}s before attempt "
                    f"{attempt + 1}/{self.max_retries} "
                    f"(total elapsed: {total_so_far:.1f}s)"
                )
                time.sleep(backoff)

        total_elapsed = time.monotonic() - call_start
        logger.error(
            f"LLM call failed after {self.max_retries} attempts "
            f"({total_elapsed:.1f}s total): {last_error}"
        )
        try:
            import perf_log as _perf
            _perf.event(
                "llm_client.call", total_elapsed,
                cache_hit=False,
                attempts=self.max_retries,
                cache_key_extra=cache_key_extra,
                model=self.model,
                error="all_attempts_failed",
            )
        except Exception:
            pass
        return None

    def call_json(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = 4096,
        use_cache: bool = True,
        cache_key_extra: str = "",
    ) -> Optional[Dict]:
        """
        Call LLM and parse response as JSON.

        Returns parsed dict, or None on failure.
        """
        text = self.call(user_message, system_prompt, max_tokens, use_cache, cache_key_extra)
        if not text:
            return None

        # Extract JSON from response (may be wrapped in markdown or explanatory text)
        json_start = text.find('{')
        json_end = text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            try:
                return json.loads(text[json_start:json_end])
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse LLM JSON response: {text[:100]}...")
                return None

        logger.warning(f"No JSON object found in LLM response: {text[:100]}...")
        return None

    @staticmethod
    def _cache_key(system_prompt: str, user_message: str, extra: str = "") -> str:
        """Generate a deterministic cache key from prompt content."""
        content = f"{system_prompt}|||{user_message}|||{extra}"
        return hashlib.sha256(content.encode()).hexdigest()

    @staticmethod
    def clear_cache():
        """Clear the response cache."""
        _response_cache.clear()


# Module-level singleton for convenience
_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get or create the shared LLM client instance."""
    global _client
    if _client is None:
        _client = LLMClient()
    return _client


def reset_client():
    """Reset the singleton (for testing or config changes)."""
    global _client
    _client = None
