import asyncio
import aiohttp
import feedparser
import logging
import time
import hashlib
import json
import email.utils
from typing import List, Set, Dict
from datetime import datetime, timezone, timedelta
from aiokafka import AIOKafkaProducer
from bs4 import BeautifulSoup
from collector_runtime.config import env_int, env_str
from collector_runtime.health import WorkloadHealth, start_health_server


DEFAULT_RSS_FEEDS = (
    "https://cointelegraph.com/rss,"
    "https://www.coindesk.com/arc/outboundfeeds/rss/,"
    "https://decrypt.co/feed"
)


def load_rss_settings() -> dict[str, object]:
    feeds = [value.strip() for value in env_str("RSS_FEEDS", DEFAULT_RSS_FEEDS).split(",") if value.strip()]
    if not feeds:
        raise ValueError("RSS_FEEDS must contain at least one URL")
    return {
        "feeds": feeds,
        "topic": env_str("RSS_TOPIC", "eth_social_stream"),
        "check_interval": env_int("RSS_CHECK_INTERVAL_SECONDS", 60),
        "bootstrap_servers": env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "health_port": env_int("HEALTH_PORT", 8080),
        "stale_after": env_int("HEALTH_STALE_AFTER_SECONDS", 180),
    }

# --- 1. 配置日志 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 定义东八区（上海时间）
SH_TZ = timezone(timedelta(hours=8))

class RSSCollector:
    def __init__(
        self,
        urls: List[str],
        bootstrap_servers: str,
        check_interval: int,
        topic: str,
        health: WorkloadHealth,
    ):
        self.urls = urls
        self.bootstrap_servers = bootstrap_servers
        self.check_interval = check_interval
        self.topic = topic
        self.health = health
        self.seen_entries: Set[str] = set()
        self.session = None
        self.producer = None

    def _generate_id(self, entry: Dict) -> str:
        """根据链接和标题生成唯一 ID，用于去重"""
        content = entry.get('link', '') + entry.get('title', '')
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    async def start(self):
        """启动异步采集引擎"""
        logger.info("🚀 启动异步采集引擎...")

        # 初始化 Kafka 异步生产者
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        self.health.mark_ready()
        logger.info(f"📡 Kafka 生产者已连接至: {self.bootstrap_servers}")

        try:
            async with aiohttp.ClientSession() as session:
                self.session = session
                while True:
                    await self.run_once()
                    self.health.heartbeat()
                    logger.info(f"💤 等待 {self.check_interval} 秒后进行下一轮抓取...")
                    await asyncio.sleep(self.check_interval)
        finally:
            await self.producer.stop()

    # 👇 注意以下代码的缩进，已将它们放入类定义中 👇
    async def fetch_and_parse(self, url: str) -> List[Dict]:
        """异步获取并解析单个 RSS 源"""
        try:
            async with self.session.get(url, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"无法访问源 {url}, 状态码: {response.status}")
                    return []

                xml_content = await response.text()
                feed = feedparser.parse(xml_content)

                new_items = []
                for entry in feed.entries:
                    entry_id = self._generate_id(entry)
                    if entry_id not in self.seen_entries:
                        # 1. 清洗 HTML 标签，仅保留 summary 纯文本
                        raw_summary = entry.get('summary', '')
                        clean_summary = BeautifulSoup(raw_summary, "html.parser").text if raw_summary else ""

                        # 2. 处理 pubDate，转换为东八区毫秒时间戳
                        pub_date_str = entry.get('published')
                        pub_ts_ms = 0
                        if pub_date_str:
                            try:
                                # 解析 RSS 标准时间字符串
                                dt = email.utils.parsedate_to_datetime(pub_date_str)
                                # 转换为上海时间并提取毫秒时间戳
                                pub_ts_ms = int(dt.astimezone(SH_TZ).timestamp() * 1000)
                            except Exception as te:
                                logger.error(f"时间解析错误: {te}")

                        # 🛑 3. 新增过滤逻辑：过滤掉 pubDate + 30分钟 < now() 的数据
                        current_ts_ms = int(time.time() * 1000)
                        thirty_mins_ms = 30 * 60 * 1000 # 30分钟的毫秒数

                        # 如果解析到了有效时间，且 (发布时间 + 30分钟) 小于 当前时间
                        if pub_ts_ms > 0 and (pub_ts_ms + thirty_mins_ms < current_ts_ms):
                            # 数据太旧，记录到已看集合中，避免下次循环重复解析
                            self.seen_entries.add(entry_id)
                            # logger.debug(f"⏳ 数据太旧已过滤 (发布时间: {pub_date_str}): {entry.get('title')}")
                            continue

                        # 4. 构造精简后的 Item
                        item = {
                            "id": entry_id,
                            "title": entry.get('title'),
                            "summary": clean_summary,
                            "pubDate": pub_ts_ms,  # 毫秒时间戳
                            "source": url,
                            "es": current_ts_ms # 采集时间戳（原 timestamp）
                        }
                        new_items.append(item)
                        self.seen_entries.add(entry_id)
                return new_items
        except Exception as e:
            logger.error(f"抓取失败 {url}: {e}")
            return []

    async def process_item(self, item: Dict):
        """发送至 Kafka"""
        try:
            await self.producer.send_and_wait(self.topic, item)
            logger.info(f"✅ 已同步至 Kafka: {item['title'][:40]}...")
        except Exception as e:
            logger.error(f"❌ Kafka 发送失败: {e}")

    async def run_once(self):
        tasks = [self.fetch_and_parse(url) for url in self.urls]
        results = await asyncio.gather(*tasks)
        all_new_items = [item for sublist in results for item in sublist]
        for item in all_new_items:
            await self.process_item(item)

if __name__ == "__main__":
    settings = load_rss_settings()
    health = WorkloadHealth(settings["stale_after"])
    start_health_server(health, settings["health_port"])
    collector = RSSCollector(
        urls=settings["feeds"],
        bootstrap_servers=settings["bootstrap_servers"],
        check_interval=settings["check_interval"],
        topic=settings["topic"],
        health=health,
    )

    try:
        asyncio.run(collector.start())
    except KeyboardInterrupt:
        logger.info("用户停止采集任务")
